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

import scala._, Predef._

import cats.data.NonEmptyList
import cats.effect._

import doobie.Transactor

import quasar.api.ColumnType
import quasar.api.destination._
import quasar.connector.MonadResourceErr
import quasar.connector.destination._
import quasar.connector.render.RenderConfig
import quasar.lib.jdbc.destination.WriteMode
import quasar.lib.jdbc.destination.flow.Retry

import org.slf4s.Logger

import scala.concurrent.duration._

final class SnowflakeDestination[F[_]: ConcurrentEffect: MonadResourceErr: Timer: ContextShift](
    val transactor: Resource[F, Transactor[F]],
    writeMode: WriteMode,
    schema: String,
    hygienicIdent: String => String,
    retryTimeout: FiniteDuration,
    maxRetries: Int,
    stagingSize: Int,
    val blocker: Blocker,
    val logger: Logger)
    extends Flow.Sinks[F] with LegacyDestination[F] {

  def destinationType: DestinationType =
    SnowflakeDestinationModule.destinationType

  def tableBuilder(args: Flow.Args, rxa: Resource[F, Transactor[F]], logger: Logger): Resource[F, TempTable.Builder[F]] = {
    val stagingParams = StageFile.Params(
      maxRetries = maxRetries,
      timeout = retryTimeout,
      maxFileSize = stagingSize)

    Resource.eval(TempTable.builder[F](
      writeMode,
      schema,
      hygienicIdent,
      Retry[F](maxRetries, retryTimeout),
      args,
      stagingParams,
      rxa,
      logger))
  }

  def render: RenderConfig[Byte] = RenderConfig.Csv(includeHeader = false, includeBom = false)

  val sinks: NonEmptyList[ResultSink[F, ColumnType.Scalar]] =
    flowSinks
}
