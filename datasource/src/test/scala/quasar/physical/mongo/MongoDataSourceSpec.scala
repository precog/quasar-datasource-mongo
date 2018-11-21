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

import cats.effect.{IO, ConcurrentEffect, ContextShift, Timer}
import quasar.EffectfulQSpec
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.{Datasource, QueryResult, ResourceError, MonadResourceErr}
import fs2.Stream
import scala.concurrent.ExecutionContext
import quasar.contrib.scalaz.MonadError_
import shims._

class MongoDataSourceSpec extends EffectfulQSpec[IO]() (MongoDataSourceSpec.F, MongoDataSourceSpec.ec) {
  import MongoDataSourceSpec._

  step(MongoSpec.setupDB)

  "pathIsResource" >> {
    "root is not a resource" >>* {
      for {
        ds <- mkDataSource
        res <- ds.pathIsResource(ResourcePath.root())
      } yield !res
    }
    "root and empty file is not a resource" >>* {
      for {
        ds <- mkDataSource
        res <- ds.pathIsResource(ResourcePath.root() / ResourceName(""))
      } yield !res
    }
    "prefix without contents is not a resource" >>* {
      val stream = for {
        ds <- dsSignal
        dbName <- MongoSpec.correctDbs.map(_.name)
        res <- Stream.eval(ds.pathIsResource(ResourcePath.root() / ResourceName(dbName)))
      } yield res
      stream.fold(false)(_ || _).map(!_).compile.last.map(_.getOrElse(false))
    }
    "existent collections are resources" >>* {
      val stream = for {
        ds <- dsSignal
        col <- MongoSpec.correctCollections
        res <- col match {
          case Collection(Database(dbName), colName) =>
            Stream.eval(ds.pathIsResource(ResourcePath.root() / ResourceName(dbName) / ResourceName(colName)))
        }
      } yield res
      stream.fold(true)(_ && _).compile.last.map(_.getOrElse(false))
    }
    "non-existent collections aren't resources" >>* {
      val stream = for {
        ds <- dsSignal
        col <- MongoSpec.incorrectCollections
        res <- col match {
          case Collection(Database(dbName), colName) =>
            Stream.eval(ds.pathIsResource(ResourcePath.root() / ResourceName(dbName) / ResourceName(colName)))
        }
      } yield res
      stream.fold(false)(_ || _).map(!_).compile.last.map(_.getOrElse(false))
    }
  }
}

object MongoDataSourceSpec {
  val ec: ExecutionContext = ExecutionContext.global
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)
  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val F = ConcurrentEffect[IO]

  def mkDataSource: IO[MongoDataSource[IO]] =
    MongoSpec.mkMongo.compile.lastOrError.flatMap(MongoDataSource[IO](_))
  def dsSignal: Stream[IO, MongoDataSource[IO]] =
    MongoSpec.mkMongo.flatMap(client => Stream.eval(MongoDataSource[IO](client)))
}
