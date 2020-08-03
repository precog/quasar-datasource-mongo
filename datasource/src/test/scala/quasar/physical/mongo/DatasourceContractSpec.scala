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

import quasar.{RateLimiter, NoopRateLimitUpdater}
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.ByteStore
import quasar.connector.datasource.{Datasource, DatasourceSpec}

import java.util.UUID
import scala.io.Source

import argonaut.Argonaut.jString
import argonaut.Json

import cats.MonadError
import cats.effect.{IO, Resource}
import cats.kernel.instances.uuid._

import fs2.Stream

import testImplicits._

class DatasourceContractSpec extends DatasourceSpec[IO, Stream[IO, ?], ResourcePathType.Physical] {

  val host = Source.fromResource("mongo-host").mkString.trim
  val port: String = "27018"

  val cfg = Json.obj(
    "connectionString" -> jString(s"mongodb://root:secret@${host}:${port}"),
    "pushdownLevel" -> jString("full"))

  val datasource =
    Resource.liftF(RateLimiter[IO, UUID](1.0, IO.delay(UUID.randomUUID()), NoopRateLimitUpdater[IO, UUID]))
      .flatMap(rl => MongoDataSourceModule.lightweightDatasource[IO, UUID](cfg, rl, ByteStore.void[IO], _ => IO(None)))
      .flatMap {
        case Right(ds) =>
          Resource.pure[IO, Datasource[Resource[IO, ?], Stream[IO, ?], _, _, ResourcePathType.Physical]](ds)

        case Left(err) =>
          MonadError[Resource[IO, ?], Throwable].raiseError(
            new RuntimeException(s"Unexpected error: $err"))
      }

  override val nonExistentPath =
    ResourcePath.root() / ResourceName("doesNotExist")

  override def gatherMultiple[A](s: Stream[IO, A]): IO[List[A]] =
    s.compile.toList

  step(MongoSpec.setupDB.unsafeRunSync())
}
