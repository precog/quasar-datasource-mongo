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
import cats.instances.list._
import cats.syntax.apply._
import cats.syntax.traverse._

import quasar.physical.mongo.MongoResource.{Collection, Database}
import quasar.{EffectfulQSpec, Disposable}

import org.bson.{Document => _, _}
import org.mongodb.scala.{MongoClient, Completed, Document}
import org.specs2.specification.core._
import org.specs2.execute.AsResult

import scala.io.Source

import shims._
import testImplicits._

class MongoSpec extends EffectfulQSpec[IO] {
  import MongoSpec._

  step(MongoSpec.setupDB)
  "can create client from valid connection string" >>*
    mkMongo.map(_ must not(throwA[Throwable]))

  "can't create client from incorrect connection string" >> {
    "for incorrect protocol" >> {
      Mongo[IO](MongoConfig("http://localhost")).unsafeRunSync() must throwA[Throwable]
    }
    "for unreachable config" >> {
      Mongo[IO](MongoConfig("mongodb://unreachable")).unsafeRunSync() must throwA[Throwable]
    }

  }

  "getting databases works correctly" >>* mkMongo.flatMap(_ { mongo =>
    mongo.databases.compile.toList.map { evaluatedDbs =>
      MongoSpec.correctDbs.toSet.subsetOf(evaluatedDbs.toSet)
    }
  })

  "databaseExists returns true for existing dbs" >> Fragment.foreach(MongoSpec.correctDbs)(db =>
      s"checking ${db.name}" >>* mkMongo.flatMap(_ { mongo =>
        mongo.databaseExists(db).compile.lastOrError
      })
  )

  "databaseExists returns false for non-existing dbs" >> Fragment.foreach(MongoSpec.incorrectDbs)(db =>
    s"checking ${db.name}" >>* mkMongo.flatMap(_ { mongo =>
      mongo.databaseExists(db).map(!_).compile.lastOrError
    })
  )

  "collections returns correct collection lists" >> Fragment.foreach(MongoSpec.correctDbs)(db =>
    s"checkgin ${db.name}" >>* mkMongo.flatMap(_ { mongo =>
      mongo.collections(db)
        .fold(List[Collection]())((lst, coll) => coll :: lst)
        .map(collectionList => collectionList.toSet === MongoSpec.cols.map(c => Collection(db, c)).toSet)
        .compile
        .lastOrError
    })
  )

  "collections return empty stream for non-existing databases" >> Fragment.foreach(MongoSpec.incorrectDbs)(db =>
    s"checking ${db.name}" >>* mkMongo.flatMap(_ { mongo =>
      mongo.collections(db).compile.toList.map(_ === List[Collection]())
    })
  )

  "collectionExists returns true for existent collections" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.flatMap(_ { mongo =>
      mongo.collectionExists(col).compile.lastOrError
    })
  )

  "collectionExists returns false for non-existent collections" >> Fragment.foreach(MongoSpec.incorrectCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.flatMap(_ { mongo =>
      mongo.collectionExists(col).map(!_).compile.lastOrError
    })
  )

  "findAll returns correct results for existing collections" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.flatMap(_ { mongo =>
      mongo.findAll(col).flatMap { stream =>
        stream
          .fold(List[BsonValue]())((lst, col) => col :: lst)
          .map(bsons => bsons match {
            case (bson: BsonDocument) :: List() => AsResult(bson.getString(col.name).getValue() === col.database.name)
            case _ => AsResult(false)
          })
          .compile
          .lastOrError
      }
    })
  )
  "findAll raises error for nonexistent collections" >> Fragment.foreach(MongoSpec.incorrectCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.flatMap(_ { mongo =>
      IO.delay(mongo.findAll(col).unsafeRunSync() must throwA[Throwable])
    })
  )
}

object MongoSpec {
  import Mongo._

  val dbs = List("A", "B", "C", "D")
  val cols = List("a", "b", "c", "d")
  val nonexistentDbs = List("Z", "Y")
  val nonexistentCols = List("z", "y")

  lazy val connectionString = Source.fromFile("./datasource/src/test/resources/mongo-connection").mkString.trim

  def mkMongo: IO[Disposable[IO, Mongo[IO]]] =
    Mongo[IO](MongoConfig(connectionString))

  def incorrectCollections: List[Collection] = {
    val incorrectDbStream =
      nonexistentDbs
        .map((dbName: String) => (colName: String) => Collection(Database(dbName), colName))
        .ap(cols ++ nonexistentCols)

    val incorrectColStream =
      nonexistentCols
        .map((colName: String) => (dbName: String) => Collection(Database(dbName), colName))
        .ap(dbs)
    incorrectDbStream ++ incorrectColStream
  }

  def correctCollections: List[Collection] =
    dbs.map((dbName: String) => (colName: String) => Collection(Database(dbName), colName)).ap(cols)

  def correctDbs: List[Database] =
    dbs.map(Database(_))

  def incorrectDbs: List[Database] =
    nonexistentDbs.map(Database(_))

  def setupDB(): Unit = {
    val ioSetup = for {
      client <- IO.delay(MongoClient(connectionString))
      _ <- correctCollections.traverse { col => for {
        mongoCollection <- IO.delay(client.getDatabase(col.database.name).getCollection(col.name))
        _ <- singleObservableAsF[IO, Completed](mongoCollection.drop).attempt
        _ <- singleObservableAsF[IO, Completed](mongoCollection.insertOne(Document(col.name -> col.database.name)))
        } yield ()
      }
      _ <- IO.delay(client.close())
    } yield ()
    ioSetup.unsafeRunSync()
  }
}
