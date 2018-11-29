/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.physical.mongo

import slamdata.Predef._

import argonaut._, argonaut.Argonaut._

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

class MongoConfigSpec extends Specification with ScalaCheck {
  "works for mongodb protocol" >> {
    Json.obj("connectionString" -> jString("mongodb://localhost"))
      .as[MongoConfig].toEither === Right(MongoConfig("mongodb://localhost"))
    Json.obj("connectionString" -> jString("mongodb://user:password@anyhost:2980/database?a=b&c=d"))
      .as[MongoConfig].toEither === Right(MongoConfig("mongodb://user:password@anyhost:2980/database?a=b&c=d"))
  }

  "sanitized config hides credentials" >> {
    val input = Json.obj("connectionString" -> jString("mongodb://user:password@anyhost:2980/database"))
    val expected = Json.obj("connectionString" -> jString("mongodb://<REDACTED>:<REDACTED>@anyhost:2980/database"))
    MongoConfig.sanitize(input) === expected
  }
  "sanitized config without credentials isn't changed" >> {
    val input = Json.obj("connectionString" -> jString("mongodb://host:1234/db?foo=bar"))
    MongoConfig.sanitize(input) === input
  }

  "json codec" >> {
    "lawful" >> prop { connectionString: String  =>
      CodecJson.codecLaw(MongoConfig.codecMongoConfig)(MongoConfig(connectionString))
    }
  }
}
