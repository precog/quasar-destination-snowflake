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

import scala.Predef.classOf
import slamdata.Predef._

import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.free.connection.unwrap

import fs2.{Pipe, Pull, Stream}
import fs2.io.file
import fs2.io.file.WriteCursor

import java.nio.file._
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

  def fromPath[F[_]: ConcurrentEffect: ContextShift](
      input: Path,
      blocker: Blocker,
      rxa: Resource[F, Transactor[F]],
      logger: Logger)
      : Resource[F, StageFile] = {

    def inputStream =
      Files.newInputStream(input)

    val debug = (s: String) => Sync[F].delay(logger.debug(s))

    val acquire: F[StageFile] = for {
      unique <- Sync[F].delay(UUID.randomUUID.toString)
      uniqueName = s"precog-$unique"
    } yield new StageFile {
      def name = uniqueName
    }

    def ingest(sf: StageFile): F[StageFile] = rxa use { xa =>
      for {
        connection <- unwrap(classOf[SnowflakeConnection]).transact(xa)
        _ <- debug(s"Starting staging to file: @~/${sf.name}")
        _ <- blocker.delay[F, Unit](connection.uploadStream("@~", "/", inputStream, sf.name, Compressed))
        _ <- debug(s"Finished staging to file: @~/${sf.name}")
      } yield sf
    }

    val release: StageFile => F[Unit] = sf => Sync[F].defer { rxa.use { xa =>
      val fragment = fr"rm" ++ sf.fragment
      debug(s"Cleaning staging file @~/${sf.name} up") >>
      fragment.query[Unit].option.void.transact(xa)
    }}

    Resource.make(acquire)(release).evalMap(ingest(_))
  }

  def retryable[F[_]: ConcurrentEffect: ContextShift: Timer](
      input: Path,
      blocker: Blocker,
      xa: Resource[F, Transactor[F]],
      logger: Logger,
      tried: Int,
      params: Params)
      : Resource[F, StageFile] =
    StageFile.fromPath(input, blocker, xa, logger).attempt flatMap {
      case Left(_) if params.maxRetries > tried =>
        Resource.eval(Timer[F].sleep(params.timeout)) >>
        retryable(input, blocker, xa, logger, tried + 1, params)
      case Left(e) =>
        Resource.eval(Sync[F].raiseError(e))
      case Right(a) =>
        a.pure[Resource[F, *]]
    }

  // It would be much clearer if we could use `_.flatMap(Stream.resourceWeak(StageFile(...)))`
  // and then `files.compile.resource.toList` but the second finalizes resources before `eval`
  def files[F[_]: ConcurrentEffect: ContextShift: Timer](
      in: Stream[F, Byte],
      blocker: Blocker,
      xa: Resource[F, Transactor[F]],
      logger: Logger,
      params: Params)
      : Resource[F, List[StageFile]] = {
    val resources = in.through(filePipe(params.maxFileSize * 1024L * 1024L, blocker))
      .map { p =>
        retryable(p, blocker, xa, logger, 0, params).evalTap({ _ =>
          file.delete[F](blocker, p)
        }).onFinalizeCase({
          case ExitCase.Completed =>
            ().pure[F]
          case ExitCase.Error(e) =>
            file.delete[F](blocker, p).attempt >>
            Sync[F].raiseError(e).void
          case ExitCase.Canceled =>
            file.delete[F](blocker, p).attempt.void
        })
      }

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

  private def filePipe[F[_]: ConcurrentEffect: ContextShift](
      max: Long,
      blocker: Blocker)
      : Pipe[F, Byte, Path] = {
    def go(inp: Stream[F, Byte], currentFile: Path, cursor: WriteCursor[F], size: Long)
        : Pull[F, Path, Unit] = if (size > max) {
      for {
        newFile <- Pull.eval(blocker.delay[F, Path](Files.createTempFile("sf-", ".tmp")))
        cursor <- Stream.resourceWeak(WriteCursor.fromPath[F](newFile, blocker)).pull.lastOrError
        _ <- Pull.output1(currentFile.toAbsolutePath)
        res <- go(inp, newFile, cursor, 0L)
      } yield res
    } else inp.pull.uncons.flatMap {
      case None =>
        Pull.output1(currentFile.toAbsolutePath) >>
        Pull.done
      case Some((chunk, tail)) =>
        cursor.writePull(chunk).flatMap { newCursor =>
          go(tail, currentFile, newCursor, chunk.size.toLong + size)
        }
    }

    in => for {
      initFile <- Stream.eval(blocker.delay[F, Path](Files.createTempFile("sf-", ".tmp")))
      cursor <- Stream.resource(WriteCursor.fromPath[F](initFile, blocker))
      res <- go(in, initFile, cursor, 0L).stream
    } yield res
  }
}
