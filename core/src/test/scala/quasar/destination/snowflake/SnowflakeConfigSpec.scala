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

import argonaut._, Argonaut._
import quasar.lib.jdbc.destination.WriteMode

import org.specs2.mutable.Specification

object SnowflakeConfigSpec extends Specification {
  "parser" >> {
    "parses a valid config" >> {
      val testConfig = Json.obj(
        "accountName" := "foo",
        "user" := "bar",
        "password" := "secret password",
        "databaseName" := "db name",
        "schema" := "public",
        "warehouse" := "warehouse name",
        "sanitizeIdentifiers" := "false",
        "retryTransactionTimeoutMs" := 1000,
        "maxTransactionReattempts" := 20,
        "stagingFileSizeMb" := 2048)


      testConfig.as[SnowflakeConfig].result must beRight(
        SnowflakeConfig(
          None,
          "foo",
          "bar",
          "secret password",
          "db name",
          Some("public"),
          "warehouse name",
          Some(false),
          Some(1000),
          Some(20),
          Some(2048)))
    }
  }

  "configToUri" >> {
    "does not include user/password or username in connection string" >> {
      val testConfig =
        SnowflakeConfig(
          Some(WriteMode.Create),
          "foo",
          "bar",
          "secret",
          "db name",
          Some("public"),
          "warehouse name",
          Some(true),
          None,
          None,
          None)

      testConfig.jdbcUri.contains("secret") must beFalse
      testConfig.jdbcUri.contains("bar") must beFalse
    }
  }
}
