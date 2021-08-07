/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.destination.snowflake

import slamdata.Predef._
import scala.Predef.classOf

import quasar.api.{Column, ColumnType}
import quasar.api.push.OffsetKey
import quasar.api.resource._
import quasar.connector._
import quasar.connector.destination.{ResultSink, WriteMode}, ResultSink.{UpsertSink, AppendSink}
import quasar.connector.render.RenderConfig

import cats.data._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.free.connection.unwrap

import fs2.{Stream, Pipe}
import fs2.concurrent.Queue

import org.slf4s.Logger

import skolems.∀

import net.snowflake.client.jdbc.SnowflakeConnection

object Flow {
  private type FlowColumn = Column[ColumnType.Scalar]

  sealed trait Args {
    def path: ResourcePath
    def columns: NonEmptyList[FlowColumn]
    def writeMode: WriteMode
    def idColumn: Option[Column[_]]
  }

  object Args {
    def ofCreate(p: ResourcePath, cs: NonEmptyList[FlowColumn]): Args = new Args {
      def path = p
      def columns = cs
      def writeMode = WriteMode.Replace
      def idColumn = None
    }

    def ofUpsert(args: UpsertSink.Args[ColumnType.Scalar]): Args = new Args {
      def path = args.path
      def columns = args.columns
      def writeMode = args.writeMode
      def idColumn = args.idColumn.some
    }

    def ofAppend(args: AppendSink.Args[ColumnType.Scalar]): Args = new Args {
      def path = args.path
      def columns = args.columns
      def writeMode = args.writeMode
      def idColumn = args.pushColumns.primary
    }
  }

  abstract class Sinks[F[_]: ConcurrentEffect: ContextShift] {
    type Consume[E[_], A] =
      Pipe[F, E[OffsetKey.Actual[A]], OffsetKey.Actual[A]]

    def tableBuilder(args: Args, xa: Transactor[F], logger: Logger): Resource[F, TempTable.Builder[F]]
    def transactor: Resource[F, Transactor[F]]
    def logger: Logger
    def blocker: Blocker

    def render: RenderConfig[Byte]

    def flowSinks: NonEmptyList[ResultSink[F, ColumnType.Scalar]] =
      NonEmptyList.of(ResultSink.create(create), ResultSink.upsert(upsert), ResultSink.append(append))

    private def create(path: ResourcePath, cols: NonEmptyList[FlowColumn])
        : (RenderConfig[Byte], Pipe[F, Byte, Unit]) = {
      val args = Args.ofCreate(path, cols)
      (render, (in: Stream[F, Byte]) => {
        val nestedStream = Stream.resourceWeak {
          for {
            xa <- transactor
            connection <- Resource.eval(unwrap(classOf[SnowflakeConnection]).transact(xa))
            builder <- tableBuilder(args, xa, logger)
            region = Region.fromByteStream(in)
            tempTable <- builder.build(connection, blocker)
          } yield tempTable.ingest(Stream(region))
        }
        nestedStream.flatten
      })
    }

    private def upsert(upsertArgs: UpsertSink.Args[ColumnType.Scalar])
        : (RenderConfig[Byte], ∀[Consume[DataEvent[Byte, *], *]]) = {
      val args = Args.ofUpsert(upsertArgs)
      val consume = ∀[Consume[DataEvent[Byte, *], *]](upsertPipe(args))
      (render, consume)
    }

    private def append(appendArgs: AppendSink.Args[ColumnType.Scalar])
        : (RenderConfig[Byte], ∀[Consume[AppendEvent[Byte, *], *]]) = {
      val args = Args.ofAppend(appendArgs)
      val consume = ∀[Consume[AppendEvent[Byte, *], *]](upsertPipe(args))
      (render, consume)
    }

    private def upsertPipe[A](args: Args): Consume[DataEvent[Byte, *], A] = { events =>
      val nestedStream = Stream.resourceWeak {
        for {
          xa <- transactor
          connection <- Resource.eval(unwrap(classOf[SnowflakeConnection]).transact(xa))
          builder <- tableBuilder(args, xa, logger)
          tempTable <- builder.build(connection, blocker)
          offsets <- Resource.eval(Queue.unbounded[F, Option[OffsetKey.Actual[A]]])
        } yield events.through(Region.regionPipe).through(tempTable.ingest)
      }

      nestedStream.flatten
    }
  }
}
