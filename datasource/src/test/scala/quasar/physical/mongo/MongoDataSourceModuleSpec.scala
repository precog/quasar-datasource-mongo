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

package quasar.physical.mongo

import slamdata.Predef._

import quasar.{EffectfulQSpec, RateLimiter, NoopRateLimitUpdater}
import quasar.api.datasource.DatasourceError
import quasar.connector.ByteStore
import quasar.physical.mongo.testImplicits._

import argonaut._, Argonaut._
import cats.effect.IO
import cats.kernel.instances.uuid._
import scalaz.NonEmptyList

import java.util.UUID

class MongoDataSourceModuleSpec extends EffectfulQSpec[IO] {
  "Using incorrect config leads to Left invalid configuration" >>* {
    val config = Json.obj("foo" -> Json.jString("bar"), "batchSize" -> Json.jNumber(64), "pushdownLevel" -> Json.jString("full"))

    RateLimiter[IO, UUID](1.0, IO.delay(UUID.randomUUID()), NoopRateLimitUpdater[IO, UUID]).flatMap(rl =>
      MongoDataSourceModule.lightweightDatasource[IO, UUID](config, rl, ByteStore.void[IO], _ => IO(None)).use(r =>
        IO(r must_=== Left(DatasourceError.InvalidConfiguration(MongoDataSource.kind, config, NonEmptyList("Attempt to decode value on failed cursor."))))))
  }

  "Using correct config produces Right Disposable" >>* {
    val config = MongoConfig.basic(MongoSpec.connectionString).withBatchSize(12).withPushdown(PushdownLevel.Full).asJson
    RateLimiter[IO, UUID](1.0, IO.delay(UUID.randomUUID()), NoopRateLimitUpdater[IO, UUID]).flatMap(rl =>
      MongoDataSourceModule.lightweightDatasource[IO, UUID](config, rl, ByteStore.void[IO], _ => IO(None)).use(r => IO(r must beRight)))
  }
}
