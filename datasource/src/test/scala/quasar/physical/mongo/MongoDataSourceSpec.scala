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
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.physical.mongo.MongoResource.{Collection, Database}
import quasar.EffectfulQSpec

import cats.effect.IO
import fs2.Stream
import org.bson._
import org.specs2.specification.core._
import testImplicits._

class MongoDataSourceSpec extends EffectfulQSpec[IO] {
  import MongoDataSourceSpec._

  def pathOfDatabase(db: Database) =
    ResourcePath.root() / ResourceName(db.name)

  def pathOfCollection(col: Collection) =
    pathOfDatabase(col.database) / ResourceName(col.name)

  def assertErrors(path: ResourcePath): IO[Boolean] = {
    val stream = for {
      ds <- dsStream
      queryRes <- Stream.eval(ds.evaluate(path))
      bson <- queryRes.data
    } yield bson

    stream
      .attempt
      .fold(true)((acc: Boolean, errOrRes: Either[Throwable, Any]) => acc && errOrRes.isLeft)
      .compile
      .lastOrError
  }

  step(MongoSpec.setupDB)

  "evaluate" >> {
    "evaluation root and weirdRoot is error stream" >> {
      assertErrors(ResourcePath.root()).unsafeRunSync()
      assertErrors(ResourcePath.root() / ResourceName("")).unsafeRunSync()
    }


    "evaluation of existing collections is collection content" >>* {
      def checkBson(col: Collection, any: Any): Boolean = any match {
        case bson: BsonValue =>
          (bson.asDocument().get(col.name).asString().getValue() === col.database.name).isSuccess
        case _ => false
      }

      val stream: Stream[IO, Boolean] = for {
        ds <- dsStream
        col <- Stream.emits(MongoSpec.correctCollections)
        queryRes <- Stream.eval(ds.evaluate(pathOfCollection(col)))
        any <- queryRes.data
        checked <- Stream.emit(checkBson(col, any))
      } yield checked

      stream.fold(true)(_ && _).compile.lastOrError
    }

    "evaluation of non-existent collections is error stream" >>
      Fragment.foreach(MongoSpec.incorrectCollections)(col =>
        s"checking ${col.database.name} :: ${col.name}" >>*
          assertErrors(pathOfCollection(col))
      )
    "evaluation of a database is error stream" >>
      Fragment.foreach(MongoSpec.dbs)(db =>
        s"checking $db" >>* assertErrors(ResourcePath.root() / ResourceName(db))
      )
  }

  "prefixedChildPaths" >> {
    def assertPrefixed(path: ResourcePath, expected: List[(ResourceName, ResourcePathType)]): IO[Boolean] = {

      val fStream = for {
        ds <- mkDataSource
        res <- ds.prefixedChildPaths(path)
      } yield res.getOrElse(Stream.empty)

      Stream.force(fStream)
        .compile
        .toList
        .map(x => expected.toSet.subsetOf(x.toSet))
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
        asserted <- Stream.eval(assertPrefixed(ResourcePath.root() / ResourceName(db), expected))
      } yield asserted
      stream.fold(true)(_ && _).compile.lastOrError
    }
    "nonexistent database children are empty" >> Fragment.foreach(MongoSpec.incorrectDbs)(db =>
      s"checking ${db.name}" >>*
        mkDataSource.flatMap(ds => ds.prefixedChildPaths(pathOfDatabase(db))).map(_.isEmpty)
    )

    "there is no children of collections" >>
      Fragment.foreach (MongoSpec.correctCollections ++ MongoSpec.incorrectCollections)(col =>
        s"checking ${col.database.name} :: ${col.name}" >>*
          mkDataSource.flatMap(ds => ds.prefixedChildPaths(pathOfCollection(col))).map(_.isEmpty)
      )
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
    "prefix without contents is not a resource" >> Fragment.foreach(MongoSpec.correctDbs) (db =>
      s"checking ${db.name}" >>*
        mkDataSource.flatMap(ds => ds.pathIsResource(pathOfDatabase(db))).map(!_)
    )
    "existent collections are resources" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
      s"checking ${col.database.name} :: ${col.name}" >>*
        mkDataSource.flatMap(ds => ds.pathIsResource(pathOfCollection(col)))
    )

    "non-existent collections aren't resources" >> Fragment.foreach(MongoSpec.incorrectCollections)(col =>
      s"checking ${col.database.name} :: ${col.name}" >>*
        mkDataSource.flatMap(ds => ds.pathIsResource(pathOfCollection(col))).map(!_)
    )
  }
}

object MongoDataSourceSpec {
  def mkDataSource: IO[MongoDataSource[IO]] =
    MongoSpec.mkMongo.compile.lastOrError.flatMap(MongoDataSource[IO](_))
  def dsStream: Stream[IO, MongoDataSource[IO]] =
    MongoSpec.mkMongo.flatMap(client => Stream.eval(MongoDataSource[IO](client)))
}
