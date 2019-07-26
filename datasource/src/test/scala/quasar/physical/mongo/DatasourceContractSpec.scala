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
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.LightweightDatasourceModule.DS
import quasar.connector.DatasourceSpec

import scala.io.Source

import argonaut.Argonaut.jString
import argonaut.Json
import cats.effect.IO
import cats.instances.tuple._
import cats.syntax.bifunctor._
import fs2.Stream
import org.specs2.specification.AfterAll
import testImplicits._

class DatasourceContractSpec extends DatasourceSpec[IO, Stream[IO, ?], ResourcePathType.Physical] with AfterAll {

  val host = Source.fromResource("mongo-host").mkString.trim
  val port: String = "27018"

  val cfg = Json.obj(
    "connectionString" -> jString(s"mongodb://root:secret@${host}:${port}"),
    "pushdownLevel" -> jString("full"))

  lazy val ds: (DS[IO], IO[Unit]) =
    MongoDataSourceModule.lightweightDatasource[IO](cfg)
      .allocated
      .unsafeRunSync()
      .leftMap(_.getOrElse(throw new RuntimeException("Unexpected error")))

  def afterAll: Unit =
    ds._2.unsafeRunSync()

  override def datasource: DS[IO] =
    ds._1

  override val nonExistentPath =
    ResourcePath.root() / ResourceName("doesNotExist")

  override def gatherMultiple[A](s: Stream[IO, A]): IO[List[A]] =
    s.compile.toList

  step(MongoSpec.setupDB.unsafeRunSync())
}
