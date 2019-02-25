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

import quasar.physical.mongo.MongoResource.{Database, Collection}

class MongoConfigSpec extends Specification with ScalaCheck {
  "works for mongodb protocol" >> {
    Json.obj("connectionString" -> jString("mongodb://localhost"))
      .as[MongoConfig].toEither === Right(MongoConfig("mongodb://localhost", None, None))
    Json.obj("connectionString" -> jString("mongodb://user:password@anyhost:2980/database?a=b&c=d"))
      .as[MongoConfig].toEither === Right(MongoConfig("mongodb://user:password@anyhost:2980/database?a=b&c=d", None, None))
  }

  "sanitized config hides credentials" >> {
    val input = Json.obj(
      "connectionString" -> jString("mongodb://user:password@anyhost:2980/database"),
      "resultBatchSizeBytes" -> jNumber(128),
      "pushdownLevel" -> jString("light"))
    val expected = Json.obj(
      "connectionString" -> jString("mongodb://<REDACTED>:<REDACTED>@anyhost:2980/database"),
      "resultBatchSizeBytes" -> jNumber(128),
      "pushdownLevel" -> jString("light"))

    MongoConfig.sanitize(input) === expected
  }

  "sanitized config without credentials isn't changed" >> {
    val input = Json.obj(
      "connectionString" -> jString("mongodb://host:1234/db?foo=bar"),
      "resultBatchSizeBytes" -> jNumber(234),
      "pushdownLevel" -> jString("full"))
    MongoConfig.sanitize(input) === input
  }

  "accessed resource" >> {
    "for default params is None" >> {
      MongoConfig("mongodb://localhost", None, None).accessedResource === None
    }
    "for db provided is Some(Database(_))" >> {
      MongoConfig("mongodb://localhost/db", None, None).accessedResource === Some(Database("db"))
    }
    "for collection provided is Some(Collection(_, _))" >> {
      MongoConfig("mongodb://localhost/db.coll", None, None).accessedResource === Some(Collection(Database("db"), "coll"))
    }
  }

  "pushdown level" >> {
    "full provided" >> {
      val input = Json.obj(
        "connectionString" -> jString("mongodb://user:password@anyhost:1234"),
        "resultBatchSizeBytes" -> jNumber(128),
        "pushdownLevel" -> jString("full"))
      input.as[MongoConfig].toEither ===
        Right(MongoConfig("mongodb://user:password@anyhost:1234", Some(128), Some(PushdownLevel.Full)))
    }
    "disabled provided" >> {
      val input = Json.obj(
        "connectionString" -> jString("mongodb://user:password@anyhost:1234"),
        "pushdownLevel" -> jString("disabled"))
      input.as[MongoConfig].toEither ===
        Right(MongoConfig("mongodb://user:password@anyhost:1234", None, Some(PushdownLevel.Disabled)))
    }
    "light provided" >> {
      val input = Json.obj(
        "connectionString" -> jString("mongodb://user:password@anyhost:1234"),
        "pushdownLevel" -> jString("light"))
      input.as[MongoConfig].toEither ===
        Right(MongoConfig("mongodb://user:password@anyhost:1234", None, Some(PushdownLevel.Light)))
    }
    "incorrect provided" >> {
      val input = Json.obj(
        "connectionString" -> jString("mongodb://user:password@anyhost:1234"),
        "pushdownLevel" -> jString("incorrect"))
      input.as[MongoConfig].toEither must beLeft
    }

    "omitted" >> {
      val input = Json.obj("connectionString" -> jString("mongodb://user:password@anyhost:1234"))
      input.as[MongoConfig].toEither === Right(MongoConfig("mongodb://user:password@anyhost:1234", None, None))
    }
  }

  "json codec" >> {
    "lawful" >> prop { params: (String, Option[Int])  =>
      CodecJson.codecLaw(MongoConfig.codecMongoConfig)(MongoConfig(params._1, params._2, None))
    }
  }
}
