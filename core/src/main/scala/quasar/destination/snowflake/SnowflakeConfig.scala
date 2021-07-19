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

import argonaut._, Argonaut._

import quasar.lib.jdbc.destination.WriteMode

import scala.concurrent.duration._

final case class SnowflakeConfig(
  writeMode: Option[WriteMode],
  accountName: String,
  user: String,
  password: String,
  databaseName: String,
  schema: Option[String],
  warehouse: String,
  sanitizeIdentifiers: Option[Boolean],
  retryTransactionTimeoutMs: Option[Int],
  maxTransactionReattempts: Option[Int],
  stagingFileSizeMb: Option[Int]) { self =>

  def sanitize: SnowflakeConfig =
    self.copy(user = SnowflakeConfig.Redacted, password = SnowflakeConfig.Redacted)

  val retryTransactionTimeout: FiniteDuration =
    retryTransactionTimeoutMs.map(_.milliseconds) getOrElse SnowflakeConfig.DefaultTimeout

  val maxRetries: Int =
    maxTransactionReattempts getOrElse SnowflakeConfig.DefaultMaxReattempts

  val stagingFileSize: Int =
    stagingFileSizeMb getOrElse SnowflakeConfig.DefaultStagingFileSize

  val jdbcUri: String =
    s"${SnowflakeConfig.SnowflakeUriSchema}://${accountName}.${SnowflakeConfig.SnowflakeDomain}/?db=${databaseName}&schema=${schema}&warehouse=${warehouse}&CLIENT_SESSION_KEEP_ALIVE=true&AUTOCOMMIT=false"
}

object SnowflakeConfig {
  val Redacted = "<REDACTED>"
  val DefaultTimeout = 60.seconds
  val DefaultMaxReattempts = 10
  val SnowflakeUriSchema = "jdbc:snowflake"
  val SnowflakeDomain = "snowflakecomputing.com"
  val DefaultStagingFileSize = 512

  implicit val snowflakeConfigCodecJson: CodecJson[SnowflakeConfig] =
    casecodec11(SnowflakeConfig.apply, SnowflakeConfig.unapply)(
      "writeMode",
      "accountName",
      "user",
      "password",
      "databaseName",
      "schema",
      "warehouse",
      "sanitizeIdentifiers",
      "retryTransactionTimeoutMs",
      "maxTransactionReattempts",
      "stagingFileSizeMb")

}
