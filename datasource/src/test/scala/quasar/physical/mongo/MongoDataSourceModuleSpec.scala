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

import argonaut._, Argonaut._

import cats.effect.IO

import quasar.physical.mongo.testImplicits._
import quasar.EffectfulQSpec

import shims._

class MongoDataSourceModuleSpec extends EffectfulQSpec[IO] {
  "Using incorrect config leads to Left error" >>* {
    val config = Json.obj("foo" -> Json.jString("bar"))
    MongoDataSourceModule.lightweightDatasource[IO](config).map (_.asCats must beLeft)
  }
  "Using correct config produces Right Disposable" >>* {
    val config = MongoConfig(MongoSpec.connectionString).asJson
    MongoDataSourceModule.lightweightDatasource[IO](config).map (_.asCats must beRight)
  }
  "Using unreachable config produces Left error" >>* {
    val config = MongoConfig("mongodb://unreachable").asJson
    MongoDataSourceModule.lightweightDatasource[IO](config).map (_.asCats must beLeft)
  }
}
