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

import cats.effect.IO

import fs2.Stream

import org.bson._
import org.specs2.specification.core._

import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.physical.mongo.MongoResource.Collection
import quasar.{EffectfulQSpec, Disposable}

import shims._
import testImplicits._

class MongoDataSourceSpec extends EffectfulQSpec[IO] {
  def mkDataSource: IO[Disposable[IO, MongoDataSource[IO]]] =
    MongoSpec.mkMongo.map(_.map(new MongoDataSource[IO](_)))

  step(MongoSpec.setupDB)

  "evaluation" >> {
    "of root should raises an error" >> {
      mkDataSource.flatMap(_ { _.evaluate(ResourcePath.root())}).unsafeRunSync() must throwA[Throwable]
    }
    "of a database raises an error" >>
      Fragment.foreach(MongoSpec.correctDbs ++ MongoSpec.incorrectDbs)(db =>
        s"checking ${db.name}" >> {
          mkDataSource.flatMap(_ { _.evaluate(db.resourcePath) }).unsafeRunSync() must throwA[Throwable]
        }
      )
    "of existing collections is collection content" >>
      Fragment.foreach(MongoSpec.correctCollections)(coll =>
        s"checking ${coll.database.name} :: ${coll.name}" >>* {
          def checkBson(col: Collection, bsons: List[Any]): Boolean = bsons match {
            case (bson: BsonValue) :: List() =>
              (bson.asDocument().get(coll.name).asString().getValue() === coll.database.name).isSuccess
            case _ => false
          }
          mkDataSource.flatMap(_ { ds =>
            val fStream: IO[Stream[IO, Any]] = ds.evaluate(coll.resourcePath).map(_.data)
            val fList: IO[List[Any]] = Stream.force(fStream).compile.toList
            fList.map(checkBson(coll, _))
          })
        })
    "of non-existent collections raises error" >>
      Fragment.foreach(MongoSpec.incorrectCollections)(col =>
        s"checking ${col.database.name} :: ${col.name}" >> {
          mkDataSource.flatMap(_ { _.evaluate(col.resourcePath) }).unsafeRunSync() must throwA[Throwable]
        }
      )
  }

  "prefixedChildPaths" >> {
    def assertPrefixed(path: ResourcePath, expected: List[(ResourceName, ResourcePathType)]): IO[Boolean] = {
      mkDataSource.flatMap(_ { ds => {
        val fStream = ds.prefixedChildPaths(path).map(_ getOrElse Stream.empty)
        Stream.force(fStream).compile.toList.map(x => expected.toSet.subsetOf(x.toSet))
      }})
    }

    "root prefixed children are databases" >>*
      assertPrefixed(
        ResourcePath.root(),
        MongoSpec.correctDbs.map(db => (ResourceName(db.name), ResourcePathType.prefix))
      )

    "existent database children are collections" >> {
      val expected =
        MongoSpec.cols.map(colName => (ResourceName(colName), ResourcePathType.leafResource))
      Fragment.foreach(MongoSpec.dbs){ db =>
        s"checking $db" >>* assertPrefixed(ResourcePath.root() / ResourceName(db), expected)
      }
    }
    "nonexistent database children are empty" >> Fragment.foreach(MongoSpec.incorrectDbs)(db =>
      s"checking ${db.name}" >>*
        mkDataSource.flatMap(_ { ds => {
          ds.prefixedChildPaths(db.resourcePath).map(_.isEmpty)
        }})
    )
    "there is no children of collections" >>
      Fragment.foreach (MongoSpec.correctCollections ++ MongoSpec.incorrectCollections)(col =>
        s"checking ${col.database.name} :: ${col.name}" >>*
        mkDataSource.flatMap(_ { ds => {
          ds.prefixedChildPaths(col.resourcePath).map(_.isEmpty)
        }})
      )
  }
  "pathIsResource" >> {
    "returns false for root" >>* {
      mkDataSource.flatMap(_ { ds => ds.pathIsResource(ResourcePath.root()).map(!_) })
    }
    "returns false for prefix without contents" >> Fragment.foreach(MongoSpec.correctDbs) (db =>
      s"checking ${db.name}" >>*
        mkDataSource.flatMap(_ { ds => ds.pathIsResource(db.resourcePath).map(!_) })
    )
    "returns false for non-existent collections" >> Fragment.foreach(MongoSpec.incorrectCollections)(col =>
      s"checking ${col.database.name} :: ${col.name}" >>*
        mkDataSource.flatMap(_ { ds => ds.pathIsResource(col.resourcePath).map(!_) })
    )
    "returns true for existent collections" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
      s"checking ${col.database.name} :: ${col.name}" >>*
        mkDataSource.flatMap(_ { ds => ds.pathIsResource(col.resourcePath) })
    )
  }
}
