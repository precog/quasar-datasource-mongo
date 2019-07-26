/*
 * Copyright 2014–2019 SlamData Inc.
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

import argonaut._, Argonaut._

import cats.effect.IO

import quasar.EffectfulQSpec
import quasar.api.datasource.DatasourceError
import quasar.physical.mongo.testImplicits._

import scalaz.NonEmptyList

class MongoDataSourceModuleSpec extends EffectfulQSpec[IO] {
  "Using incorrect config leads to Left invalid configuration" >>* {
    val config = Json.obj("foo" -> Json.jString("bar"), "batchSize" -> Json.jNumber(64), "pushdownLevel" -> Json.jString("full"))
    MongoDataSourceModule.lightweightDatasource[IO](config).use(r => IO(r must_===
      Left(DatasourceError.InvalidConfiguration(MongoDataSource.kind, config, NonEmptyList("Attempt to decode value on failed cursor.")))))
  }

  "Using correct config produces Right Disposable" >>* {
    val config = MongoConfig(MongoSpec.connectionString, 12, PushdownLevel.Full, None).asJson
    MongoDataSourceModule.lightweightDatasource[IO](config).use(r => IO(r must beRight))
  }

  "Using unreachable config produces Left invalid configuration" >>* {
    val config = MongoConfig("mongodb://unreachable/?serverSelectionTimeoutMS=1000", 1, PushdownLevel.Disabled, None).asJson
    MongoDataSourceModule.lightweightDatasource[IO](config).use(r => IO(r must beLike {
      case Left(DatasourceError.ConnectionFailed(MongoDataSource.kind, cfg, _)) =>
        cfg must_=== config
    }))
  }
}
