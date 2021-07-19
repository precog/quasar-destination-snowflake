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

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{Chunk, Pipe, Pull, Stream}
import fs2.concurrent.{NoneTerminatedQueue, Queue}
import fs2.io

import java.util.UUID
import net.snowflake.client.jdbc.SnowflakeConnection
import scala.concurrent.duration._
import org.slf4s.Logger

sealed trait StageFile {
  def name: String
  def fragment = fr0"@~/" ++ Fragment.const0(name)
}

object StageFile {
  private val Compressed = true

  final case class Params(
      maxRetries: Int,
      timeout: FiniteDuration,
      maxFileSize: Int)

  def apply[F[_]: ConcurrentEffect: ContextShift](
      input: Stream[F, Byte],
      connection: SnowflakeConnection,
      blocker: Blocker,
      xa: Transactor[F],
      logger: Logger)
      : Resource[F, StageFile] = {
    val debug = (s: String) => Sync[F].delay(logger.debug(s))

    val acquire: F[StageFile] = for {
      unique <- Sync[F].delay(UUID.randomUUID.toString)
      uniqueName = s"precog-$unique"
    } yield new StageFile {
      def name = uniqueName
    }

    def ingest(sf: StageFile): F[StageFile] =
      io.toInputStreamResource(input) use { inputStream =>
        for {
          _ <- debug(s"Starting staging to file: @~/${sf.name}")
          _ <- blocker.delay[F, Unit](connection.uploadStream("@~", "/", inputStream, sf.name, Compressed))
          _ <- debug(s"Finished staging to file: @~/${sf.name}")
        } yield sf
      }

    val release: StageFile => F[Unit] = sf => Sync[F].defer {
      val fragment = fr"rm" ++ sf.fragment
      debug(s"Cleaning staging file @~/${sf.name} up") >>
      fragment.query[Unit].option.void.transact(xa)
    }

    Resource.make(acquire)(release).evalMap(ingest(_))
  }

  def retryable[F[_]: ConcurrentEffect: ContextShift: Timer](
      input: Stream[F, Byte],
      connection: SnowflakeConnection,
      blocker: Blocker,
      xa: Transactor[F],
      logger: Logger,
      tried: Int,
      params: Params)
      : Resource[F, StageFile] = for {
    // First we need two copies of input stream
    // one because on error in `StageFile` the stream is terminated
    direct <- Resource.eval(Queue.noneTerminated[F, Chunk[Byte]])
    // one to reuse it on error
    fallback <- Resource.eval(Queue.noneTerminated[F, Chunk[Byte]])
    // Then, since we don't want input stream to terminate but put all its data
    // into both `direct` and `fallback` we need to `spawn` the stream
    // the fiber will be joined in this resource finalizer.
    _ <- input
      .chunks
      .evalTap(x => direct.enqueue1(Some(x)) >> fallback.enqueue1(Some(x)))
      .onFinalize(direct.enqueue1(None) >> fallback.enqueue1(None))
      .spawn
      .compile
      .resource
      .lastOrError

    res <-
      StageFile[F](direct.dequeue.flatMap(Stream.chunk), connection, blocker, xa, logger).attempt flatMap {
        case Left(_) if params.maxRetries > tried =>
          Resource.eval(Timer[F].sleep(params.timeout)) >>
          retryable(fallback.dequeue.flatMap(Stream.chunk), connection, blocker, xa, logger, tried + 1, params)
        case Left(e) =>
          Resource.eval(Sync[F].raiseError(e))
        case Right(a) =>
          a.pure[Resource[F, *]]
      }

  } yield res

  // It would be much clearer if we could use `_.flatMap(Stream.resourceWeak(StageFile(...)))`
  // and then `files.compile.resource.toList` but the second finalizes resources before `eval`
  def files[F[_]: ConcurrentEffect: ContextShift: Timer](
      in: Stream[F, Byte],
      connection: SnowflakeConnection,
      blocker: Blocker,
      xa: Transactor[F],
      logger: Logger,
      params: Params)
      : Resource[F, List[StageFile]] = {
    val resources = in.through(split(params.maxFileSize * 1024L * 1024L))
      .map(retryable(_, connection, blocker, xa, logger, 0, params))

    def go(inp: Stream[F, Resource[F, StageFile]], acc: Resource[F, List[StageFile]])
        : Pull[F, Resource[F, List[StageFile]], Unit] = inp.pull.uncons1 flatMap {
      case None => Pull.output1(acc) >> Pull.done
      case Some((hd, tail)) =>
        val newAcc: Resource[F, List[StageFile]] = for {
          lst <- acc
          sf <- hd
        } yield lst :+ sf
        go(tail, newAcc)
    }
    Resource.suspend {
      go(resources, List.empty[StageFile].pure[Resource[F, *]]).stream.compile.lastOrError
    }
  }

  val Max = 128 * 1024L * 1024L

  private def split[F[_]: ConcurrentEffect: ContextShift, A](
      max: Long)
      : Pipe[F, A, Stream[F, A]] = in => Stream.force {
    for {
      q <- Queue.noneTerminated[F, Stream[F, A]]
      elqRef <- Ref.of[F, Option[NoneTerminatedQueue[F, Chunk[A]]]](None)
      size <- Ref.of[F, Long](0L)
    } yield {
      def fin = Sync[F].defer {
        q.enqueue1(none[Stream[F, A]]) >>
        elqRef.get.flatMap {
          case None =>
            ().pure[F]
          case Some(elq) =>
            elq.enqueue1(none[Chunk[A]])
        } >>
        size.set(0L)
      }

      q.dequeue concurrently {
        def go(inp: Stream[F, A]): Pull[F, Unit, Unit] = inp.pull.uncons flatMap {
          case None =>
            Pull.eval(fin) >> Pull.done
          case Some((chunk, tail)) =>
            def startStream: F[Unit] = for {
              elq <- Queue.noneTerminated[F, Chunk[A]]
              _ <- elqRef.set(elq.some)
              _ <- q.enqueue1(elq.dequeue.flatMap(Stream.chunk).some)
              _ <- elq.enqueue1(chunk.some)
              _ <- size.set(chunk.size.toLong)
            } yield ()
            val action: F[Unit] =
              elqRef.get flatMap {
                case None =>
                  startStream
                case Some(elq) => for {
                  currentSize <- size.get
                  _ <- if (currentSize > max) {
                    startStream >>
                    elq.enqueue1(none[Chunk[A]])
                  } else {
                    elq.enqueue1(chunk.some) >>
                    size.set(currentSize + chunk.size.toLong)
                  }
                } yield ()
              }
            Pull.eval(action) >> go(tail)
          }
        go(in).stream
      }
    }
  }
}
