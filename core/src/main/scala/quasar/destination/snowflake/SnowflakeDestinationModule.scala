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

import scala.Predef._
import scala._

import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.{DestinationError, DestinationType}
import quasar.concurrent._
import quasar.connector.{GetAuth, MonadResourceErr}
import quasar.connector.destination.{Destination, DestinationModule, PushmiPullyu}
import quasar.concurrent._
import quasar.lib.jdbc.destination.WriteMode

import scala.util.{Either, Random}

import argonaut._, Argonaut._

import cats.data.EitherT
import cats.effect._
import cats.syntax.applicative._

import doobie.util.transactor.Transactor

import org.slf4s.LoggerFactory

object SnowflakeDestinationModule extends DestinationModule {

  type InitErr = InitializationError[Json]

  def destinationType: DestinationType =
    DestinationType("snowflake", 1L)

  val SnowflakeDriverFqcn = "net.snowflake.client.jdbc.SnowflakeDriver"

  def sanitizeDestinationConfig(config: Json): Json =
    config.as[SnowflakeConfig].result.fold(_ => Json.jEmptyObject, cfg => cfg.sanitize.asJson)

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json,
      pushPull: PushmiPullyu[F],
      auth: GetAuth[F])
      : Resource[F, Either[InitErr, Destination[F]]] = {


    val init = for {
      cfg <- EitherT.fromEither[Resource[F, ?]](config.as[SnowflakeConfig].result) leftMap {
        case (err, _) => DestinationError.malformedConfiguration((
          destinationType,
          sanitizeDestinationConfig(config),
          err))
      }
      poolSuffix <- EitherT.right(Resource.eval(Sync[F].delay(Random.alphanumeric.take(5).mkString)))
      transactPool <- EitherT.right(Blocker.cached[F](s"snowflake-dest-transact-$poolSuffix"))

      jdbcUri = cfg.jdbcUri

      logger <- EitherT.right[InitializationError[Json]]{
        Resource.eval(Sync[F].delay(LoggerFactory(s"quasar.lib.destination.snowflake-$poolSuffix")))
      }
      blocker <- EitherT.right[InitializationError[Json]](Blocker.cached("snowflake-destination"))
    } yield {
      val hygienicIdent: String => String = inp =>
        QueryGen.sanitizeIdentifier(inp, cfg.sanitizeIdentifiers.getOrElse(true))

      val transactor =
        Transactor.fromDriverManager[F](
          SnowflakeDriverFqcn,
          jdbcUri,
          cfg.user,
          cfg.password,
          transactPool)

      new SnowflakeDestination(
        transactor.pure[Resource[F, ?]],
        cfg.writeMode.getOrElse(WriteMode.Replace),
        cfg.schema.getOrElse("public"),
        hygienicIdent,
        cfg.retryTransactionTimeout,
        cfg.maxRetries,
        cfg.stagingFileSize,
        blocker,
        logger): Destination[F]
    }

    init.value
  }
}
