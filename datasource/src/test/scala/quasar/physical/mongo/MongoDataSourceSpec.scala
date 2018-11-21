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
import org.specs2.matcher.MatchResult
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.ResourceError
import org.bson._
import fs2.Stream
import scala.concurrent.ExecutionContext
import quasar.contrib.scalaz.MonadError_
import shims._

class MongoDataSourceSpec extends EffectfulQSpec[IO]() (MongoDataSourceSpec.F, MongoDataSourceSpec.ec) {
  import MongoDataSourceSpec._

  step(MongoSpec.setupDB)

  "evaluate" >> {
    def assertOnlyErrors(paths: List[ResourcePath]) = {
      val stream = for {
        ds <- dsSignal
        path <- Stream.emits(paths)
        queryRes <- Stream.eval(ds.evaluate(path))
        bson <- queryRes.data
      } yield bson

      stream.attempt.compile.toList.map(_.length === paths.length)

      stream
        .attempt
        .fold(true)((acc: Boolean, errOrRes: Either[Throwable, Any]) => acc && errOrRes.isLeft)
        .compile
        .last
        .map(_.getOrElse(false))
    }
    def pathOfCollection(col: Collection) =
      ResourcePath.root() / ResourceName(col.database.name) / ResourceName(col.name)

    "evaluation root and weirdRoot is error stream" >>* {
      val roots = List(ResourcePath.root(), ResourcePath.root() / ResourceName(""))
      val paths = MongoSpec.dbs.map(db => ResourcePath.root() / ResourceName(db)) ++ roots
      assertOnlyErrors(paths)
    }
    "evaluation of existing collections is collection content" >>* {
      def checkBson(col: Collection, any: Any): Boolean = any match {
        case bson: BsonValue =>
          (bson.asDocument().get(col.name).asString().getValue() === col.database.name).isSuccess
        case _ => false
      }

      val stream: Stream[IO, Boolean] = for {
        ds <- dsSignal
        col <- MongoSpec.correctCollections
        queryRes <- Stream.eval(ds.evaluate(pathOfCollection(col)))
        any <- queryRes.data
        checked <- Stream.emit(checkBson(col, any))
      } yield checked

      stream.fold(true)(_ && _).compile.last.map(_.getOrElse(false))
    }

    "evaluation of non-existent collections is error stream" >>* {
      MongoSpec
        .incorrectCollections
        .compile
        .toList
        .map(_.map(pathOfCollection(_)))
        .flatMap(ps => assertOnlyErrors(ps))
    }
  }


  "prefixedChildPaths" >> {
    def assertPrefixed(path: ResourcePath, expected: List[(ResourceName, ResourcePathType)])
        : IO[MatchResult[Set[(ResourceName, ResourcePathType)]]] = {
      val fStream = for {
        ds <- mkDataSource
        res <- ds.prefixedChildPaths(path)
      } yield res.getOrElse(Stream.empty)

      Stream.force(fStream)
        .compile
        .toList
        .map(x => x.toSet === expected.toSet)
    }

    "root prefixed children are databases" >>* {
      val expected =
        (MongoSpec.dbs ++ List("local", "admin"))
          .map(x => (ResourceName(x), ResourcePathType.prefix))
      assertPrefixed(ResourcePath.root(), expected)
    }
    "weird root prefixed children are databases" >>* {
      val expected =
        (MongoSpec.dbs ++ List("local", "admin"))
          .map(x => (ResourceName(x), ResourcePathType.prefix))

      assertPrefixed(ResourcePath.root() / ResourceName(""), expected)
    }
    "existent database children are collections" >>* {
      val expected =
        MongoSpec.cols.map(colName => (ResourceName(colName), ResourcePathType.leafResource))
      val stream = for {
        db <- Stream.emits(MongoSpec.dbs)
        match_ <- Stream.eval(assertPrefixed(ResourcePath.root() / ResourceName(db), expected))
      } yield match_.isSuccess
      stream.fold(true)(_ && _).compile.last.map(_.getOrElse(false))
    }
    "nonexistent database children are empty" >>* {
      val stream: Stream[IO, Boolean] = for {
        ds <- dsSignal
        db <- Stream.emits(MongoSpec.nonexistentDbs)
        prefixed <- Stream.eval(ds.prefixedChildPaths(ResourcePath.root() / ResourceName(db)))
      } yield prefixed.isEmpty
      stream.map(x => scala.Predef.println(x)).drain
      stream.fold(true)(_ && _).compile.last.map(_.getOrElse(false))
    }
    "there is no children of collections" >>* {
      val stream: Stream[IO, Boolean] = for {
        ds <- dsSignal
        col <- MongoSpec.correctCollections ++ MongoSpec.incorrectCollections
        prefixed <- col match {
          case Collection(Database(dbName), colName) =>
            Stream.eval(ds.prefixedChildPaths(ResourcePath.root() / ResourceName(dbName) / ResourceName(colName)))
        }
      } yield prefixed.isEmpty
      stream.fold(true)(_ && _).compile.last.map(_.getOrElse(false))
    }

  }

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
