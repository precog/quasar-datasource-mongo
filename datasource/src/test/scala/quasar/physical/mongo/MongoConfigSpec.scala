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
  "legacy" >> {
    "no pushdown" >> {
      val expected = MongoConfig("mongodb://localhost", 64, PushdownLevel.Disabled, None)
      Json.obj(
        "connectionString" -> jString("mongodb://localhost"),
        "resultBatchSizeBytes" -> jNumber(128))
        .as[MongoConfig].toEither === Right(expected)
    }
  }

  "works for mongodb protocol" >> {
    val expected1 = MongoConfig("mongodb://localhost", 64, PushdownLevel.Light, None)
    val expected2 = MongoConfig("mongodb://user:password@anyhost:2980/database?a=b&c=d", 0, PushdownLevel.Full, None)
    Json.obj(
      "connectionString" -> jString("mongodb://localhost"),
      "batchSize" -> jNumber(64),
      "pushdownLevel" -> jString("light"))
      .as[MongoConfig].toEither === Right(expected1)
    Json.obj(
      "connectionString" -> jString("mongodb://user:password@anyhost:2980/database?a=b&c=d"),
      "batchSize" -> jNumber(0),
      "pushdownLevel" -> jString("full"))
      .as[MongoConfig].toEither === Right(expected2)
  }

  "sanitized config hides credentials" >> {
    val input = Json.obj(
      "connectionString" -> jString("mongodb://user:password@anyhost:2980/database"),
      "batchSize" -> jNumber(128),
      "pushdownLevel" -> jString("light"))
    val expected = Json.obj(
      "connectionString" -> jString("mongodb://<REDACTED>:<REDACTED>@anyhost:2980/database"),
      "batchSize" -> jNumber(128),
      "pushdownLevel" -> jString("light"),
      "tunnelConfig" -> jNull)

    MongoConfig.sanitize(input) === expected
  }

  "sanitized config without credentials isn't changed" >> {
    val input = Json.obj(
      "connectionString" -> jString("mongodb://host:1234/db?foo=bar"),
      "batchSize" -> jNumber(234),
      "pushdownLevel" -> jString("full"),
      "tunnelConfig" -> jNull)
    MongoConfig.sanitize(input) === input
  }

  "sanitized config hides tunnel password" >> {
    val input = Json.obj(
      "connectionString" -> jString("mongodb://localhost:27017"),
      "batchSize" -> jNumber(128),
      "pushdownLevel" -> jString("disabled"),
      "tunnelConfig" -> Json.obj(
        "user" -> jString("user"),
        "host" -> jString("host"),
        "port" -> jNumber(22),
        "pass" -> Json.obj("password" -> jString("should be redacted"))))
    val expected = Json.obj(
      "connectionString" -> jString("mongodb://localhost:27017"),
      "batchSize" -> jNumber(128),
      "pushdownLevel" -> jString("disabled"),
      "tunnelConfig" -> Json.obj(
        "user" -> jString("user"),
        "host" -> jString("host"),
        "port" -> jNumber(22),
        "pass" -> Json.obj("password" -> jString("<REDACTED>"))))
    MongoConfig.sanitize(input) === expected
  }

  "sanitized config hides tunnel identity" >> {
    val input = Json.obj(
      "connectionString" -> jString("mongodb://localhost:27017"),
      "batchSize" -> jNumber(128),
      "pushdownLevel" -> jString("disabled"),
      "tunnelConfig" -> Json.obj(
        "user" -> jString("user"),
        "host" -> jString("host"),
        "port" -> jNumber(22),
        "pass" -> Json.obj(
          "prv" -> jString("should be redacted"),
          "passphrase" -> jString("should be redacted"))))
    val expected = Json.obj(
      "connectionString" -> jString("mongodb://localhost:27017"),
      "batchSize" -> jNumber(128),
      "pushdownLevel" -> jString("disabled"),
      "tunnelConfig" -> Json.obj(
        "user" -> jString("user"),
        "host" -> jString("host"),
        "port" -> jNumber(22),
        "pass" -> Json.obj(
          "prv" -> jString("<REDACTED>"),
          "passphrase" -> jString("<REDACTED>"))))
    MongoConfig.sanitize(input) === expected
  }

  "accessed resource" >> {
    "for default params is None" >> {
      MongoConfig("mongodb://localhost", 16, PushdownLevel.Disabled, None).accessedResource === None
    }
    "for db provided is Some(Database(_))" >> {
      MongoConfig("mongodb://localhost/db", 64, PushdownLevel.Light, None).accessedResource === Some(Database("db"))
    }
    "for collection provided is Some(Collection(_, _))" >> {
      MongoConfig("mongodb://localhost/db.coll", 128, PushdownLevel.Full, None).accessedResource === Some(Collection(Database("db"), "coll"))
    }
  }

  "pushdown level" >> {
    "full provided" >> {
      val input = Json.obj(
        "connectionString" -> jString("mongodb://user:password@anyhost:1234"),
        "batchSize" -> jNumber(128),
        "pushdownLevel" -> jString("full"))
      input.as[MongoConfig].toEither ===
        Right(MongoConfig("mongodb://user:password@anyhost:1234", 128, PushdownLevel.Full, None))
    }
    "disabled provided" >> {
      val input = Json.obj(
        "connectionString" -> jString("mongodb://user:password@anyhost:1234"),
        "batchSize" -> jNumber(12),
        "pushdownLevel" -> jString("disabled"))
      input.as[MongoConfig].toEither ===
        Right(MongoConfig("mongodb://user:password@anyhost:1234", 12, PushdownLevel.Disabled, None))
    }
    "light provided" >> {
      val input = Json.obj(
        "connectionString" -> jString("mongodb://user:password@anyhost:1234"),
        "batchSize" -> jNumber(142),
        "pushdownLevel" -> jString("light"))
      input.as[MongoConfig].toEither ===
        Right(MongoConfig("mongodb://user:password@anyhost:1234", 142, PushdownLevel.Light, None))
    }
    "incorrect provided" >> {
      val input = Json.obj(
        "connectionString" -> jString("mongodb://user:password@anyhost:1234"),
        "batchSize" -> jNumber(0),
        "pushdownLevel" -> jString("incorrect"))
      input.as[MongoConfig].toEither must beLeft
    }
  }

  "json encode/decode roundtrip is ok" >> prop { params: (String, Int)  =>
    val cfg = MongoConfig(params._1, params._2, PushdownLevel.Disabled, None)
    cfg.asJson.as[MongoConfig].toEither === Right(cfg)
  }
}
