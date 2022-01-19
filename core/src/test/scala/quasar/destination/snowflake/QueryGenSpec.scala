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

import org.specs2.mutable.Specification

object QueryGenSpec extends Specification {
  "snowflakeSanitation = false" >> {
    "wraps with double quotes and escape double quotes with double quotes" >> {
      QueryGen.sanitizeIdentifier("includes \" spaces", false) must_== """"includes "" spaces""""
    }
  }

  "snowflakeSanitation = true" >> {
    "makes everything uppercase and replace all non-ASCII characters with underscores" >> {
      QueryGen.sanitizeIdentifier("? foo>>bar123 \";;", true) must_== "\"__FOO__BAR123____\""
    }
  }
}
