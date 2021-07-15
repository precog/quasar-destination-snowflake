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

import quasar.connector._

import cats.Applicative
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

import fs2.{Chunk, Pipe, Stream}
import fs2.concurrent.{InspectableQueue, NoneTerminatedQueue, Queue}

sealed trait Region[F[_], A] {
  def data: Stream[F, Byte]
  def commits: Stream[F, A]
  def commitCount: F[Int]
}

object Region {
  val ResultQueueSize = 256
  val InnerQueueSize = 128

  def fromByteStream[F[_]: Applicative](in: Stream[F, Byte]): Region[F, Unit] = new Region[F, Unit] {
    def data = in
    def commits = Stream(())
    def commitCount = 1.pure[F]
  }

  private[snowflake] final case class RegionState[F[_]: Concurrent, A](
      elements: NoneTerminatedQueue[F, Chunk[Byte]],
      // There's no NoneTerminatedInspectableQueue :(
      commits: InspectableQueue[F, Option[A]],
      collecting: Ref[F, Boolean]) { self =>

    def stop: F[Unit] =
      elements.enqueue1(none[Chunk[Byte]]) >>
      commits.enqueue1(none[A])

    def toRegion: Region[F, A] = new Region[F, A] {
      def data = elements.dequeue.flatMap(Stream.chunk)
      def commits = self.commits.dequeue.unNoneTerminate
      def commitCount = self.commits.getSize.map(_ - 1) // None is in queue too
    }
  }
  object RegionState {
    def apply[F[_]: Concurrent, A]: F[RegionState[F, A]] = for {
      els <- Queue.boundedNoneTerminated[F, Chunk[Byte]](InnerQueueSize)
      coms <- InspectableQueue.unbounded[F, Option[A]]
      collecting <- Ref.of[F, Boolean](true)
    } yield RegionState(els, coms, collecting)
  }

  def regionPipe[F[_]: Concurrent, A]: Pipe[F, DataEvent[Byte, A], Region[F, A]] = {
    inp => for {
      resQ <- Stream.eval(Queue.boundedNoneTerminated[F, Region[F, A]](ResultQueueSize))
      initRegion <- Stream.eval(RegionState[F, A])
      regionAccum <- Stream.eval(Ref.of[F, RegionState[F, A]](initRegion))
      _ <- Stream.eval(resQ.enqueue1(initRegion.toRegion.some))
      results <- resQ.dequeue.concurrently {
        val inpWithFinalizer = inp.onFinalize[F] {
          regionAccum.get.flatMap(_.stop) >>
          resQ.enqueue1(None)
        }
        inpWithFinalizer.through(regionPipeEnqueue(resQ, regionAccum))
      }
    } yield results
  }

  private def regionPipeEnqueue[F[_]: Concurrent, A](
      resQ: NoneTerminatedQueue[F, Region[F, A]],
      regionAccum: Ref[F, RegionState[F, A]])
      : Pipe[F, DataEvent[Byte, A], Unit] = {

    def startRegion(chunk: Chunk[Byte]): F[Unit] = for {
      _ <- regionAccum.get.flatMap(_.stop)
      newRegionState <- RegionState[F, A]
      _ <- newRegionState.elements.enqueue1(chunk.some)
      _ <- regionAccum.set(newRegionState)
      _ <- resQ.enqueue1(newRegionState.toRegion.some)
    } yield ()

    _.evalMap {
      case DataEvent.Create(chunk) => regionAccum.get flatMap { rr =>
        rr.collecting.get.ifM(
          rr.elements.enqueue1(chunk.some),
          startRegion(chunk))
      }

      case DataEvent.Delete(_) =>
        ().pure[F]

      case DataEvent.Commit(offset) => regionAccum.get flatMap { rr =>
        rr.commits.enqueue1(offset.some) >>
        rr.collecting.set(false)
      }
    }
  }
}
