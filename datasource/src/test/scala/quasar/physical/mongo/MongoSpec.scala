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
import quasar.physical.mongo.MongoResource.{Collection, Database}
import quasar.EffectfulQSpec

import cats.instances.list._
import cats.syntax.apply._
import cats.effect.IO
import fs2.Stream
import org.specs2.specification.core._
import org.specs2.execute.AsResult
import org.mongodb.scala._
import org.bson.{Document => _, _}
import scala.io.Source
import shims._
import testImplicits._

class MongoSpec extends EffectfulQSpec[IO] {
  import MongoSpec._

  step(MongoSpec.setupDB)

  "can create client from valid connection string" >> {
    mkMongo.attempt.compile.last.unsafeRunSync() match {
      case None => AsResult(false).updateMessage("Impossible happened, Mongo.apply.attempt must return something")
      case Some(Left(thr)) => AsResult(false).updateMessage("Mongo.apply returned an error" ++ thr.getMessage())
      case Some(Right(_)) => AsResult(true)
    }
  }

  "getting databases works correctly" >> {
    val stream = for {
      mongo <- mkMongo
      databases <- mongo.databases
    } yield databases
    val evaluatedDbs = stream.compile.toList.unsafeRunSync()
    MongoSpec.correctDbs.toSet.subsetOf(evaluatedDbs.toSet)
  }

  "databaseExists returns true for existing dbs" >> Fragment.foreach(MongoSpec.correctDbs)(db =>
      s"checking ${db.name}" >>* mkMongo.flatMap(mongo => mongo.databaseExists(db)).compile.lastOrError
  )

  "databaseExists returns false for non-existing dbs" >> Fragment.foreach(MongoSpec.incorrectDbs)(db =>
    s"checking ${db.name}" >>*
      mkMongo.flatMap(mongo => mongo.databaseExists(db)).map(!_).compile.lastOrError
  )

  "collections returns correct collection lists" >>
    Fragment.foreach(MongoSpec.correctDbs)(db =>
      s"checkgin ${db.name}" >>*
        mkMongo.flatMap(mongo =>
          mongo
            .collections(db)
            .fold(List[Collection]())((lst, coll) => coll :: lst))
          .map(collectionList => collectionList.toSet === MongoSpec.cols.map(c => Collection(db, c)).toSet)
          .compile
          .lastOrError
  )

  "collections return error stream for non-existing databases" >> Fragment.foreach(MongoSpec.incorrectDbs)(db =>
    s"checking ${db.name}" >>*
      mkMongo.flatMap(mongo => mongo.collections(db)).compile.toList.map (_ === List[Collection]())
  )

  "collectionExists returns true for existent collections" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>*
      mkMongo.flatMap(mongo => mongo.collectionExists(col)).compile.lastOrError
  )

  "collectionExists returns false for non-existent collections" >> Fragment.foreach(MongoSpec.incorrectCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>*
      mkMongo.flatMap(mongo => mongo.collectionExists(col)).map(!_).compile.lastOrError
  )

  "findAll returns correct results for existing collections" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>*
      mkMongo.flatMap(mongo =>
        mongo.findAll(col).fold(List[BsonValue]())((lst, col) => col :: lst))
      .map(bsons => bsons match {
        case (bson: BsonDocument) :: List() => AsResult(bson.getString(col.name).getValue() === col.database.name)
        case _ => AsResult(false)
      })
      .compile
      .lastOrError
  )

  "findAll returns correct result for nonexisting collections" >> {
    def checkFindAll(client: Mongo[IO], col: Collection): Stream[IO, Boolean] =
      client
        .findAll(col)
        .attempt
        .fold(List[Either[Throwable, BsonValue]]())((lst, col) => col :: lst)
        .map(_ match {
          case Left(_) :: List() => true
          case _ => false
        })
    Fragment.foreach(MongoSpec.incorrectCollections)(col =>
      s"checking ${col.database.name} :: ${col.name}" >>* {
        val stream = for {
          mongo <- mkMongo
          correct <- checkFindAll(mongo, col)
        } yield correct
        stream.compile.lastOrError
      }
    )
  }

  "raise errors when mongodb is unreachable" >>  {
    val unreachableURI = "mongodb://unreachable"
    Mongo[IO](MongoConfig(unreachableURI)).attempt.compile.last.unsafeRunSync() match {
      case None => AsResult(false).updateMessage("Impossible happened, Mongo.apply.attempt must return something")
      case Some(Left(_)) => AsResult(true)
      case Some(Right(_)) => AsResult(false).updateMessage("Mongo.apply.attempt worked for incorrect connection string")
    }
  }
}

object MongoSpec {
  import Mongo._

  val dbs = List("A", "B", "C", "D")
  val cols = List("a", "b", "c", "d")
  val nonexistentDbs = List("Z", "Y")
  val nonexistentCols = List("z", "y")

  lazy val connectionString = Source.fromFile("./datasource/src/test/resources/mongo-connection").mkString.trim

  def mkMongo: Stream[IO, Mongo[IO]] =
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
    val stream = for {
      client <- Stream.eval(IO.delay(MongoClient(connectionString)))
      dbName <- Stream.emits(dbs)
      colName <- Stream.emits(cols)
      db <- Stream.eval(IO.delay(client.getDatabase(dbName)))
      coll <- Stream.eval(IO.delay(db.getCollection(colName)))
      _ <- observableAsStream[IO, Completed](coll.drop).attempt
      _ <- observableAsStream[IO, Completed](coll.insertOne(Document(colName -> dbName)))
    } yield ()

    stream.compile.drain.unsafeRunSync()
  }
}
