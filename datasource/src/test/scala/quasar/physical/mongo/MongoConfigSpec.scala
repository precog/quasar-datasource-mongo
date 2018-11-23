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

import argonaut.Argonaut._
import argonaut.Json
import org.specs2.mutable.Specification

class MongoConfigSpec extends Specification {
  "works for mongodb protocol" >> {
    Json.obj("connectionString" -> jString("mongodb://localhost"))
      .as[MongoConfig].toEither must beRight
    Json.obj("connectionString" -> jString("mongodb://user:password@anyhost:2980/database?a=b&c=d"))
      .as[MongoConfig].toEither must beRight
  }
  "fails for other protocols" >> {
    Json.obj("connectionString" -> jString("http://192.168.0.1:27017"))
      .as[MongoConfig].toEither must beLeft
    Json.obj("connectionString" -> jString("ftp://foo.bar/baz"))
      .as[MongoConfig].toEither must beLeft
  }
  "fails when credentials is incomplete" >> {
    Json.obj("connectionString" -> jString("mongodb://user@host"))
      .as[MongoConfig].toEither must beLeft
  }
  "sanitized config hides credentials" >> {
    val input = Json.obj("connectionString" -> jString("mongodb://user:password@anyhost:2980/database"))
    val expected = Json.obj("connectionString" -> jString("mongodb://<hidden>:<hidden>@anyhost:2980/database"))
    MongoConfig.sanitize(input) === expected
  }
  "sanitized config without credentials isn't changed" >> {
    val input = Json.obj("connectionString" -> jString("mongodb://host:1234/db?foo=bar"))
    MongoConfig.sanitize(input) === input
  }
}
