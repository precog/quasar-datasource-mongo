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

import quasar.connector.ResourceError
import quasar.physical.mongo.MongoResource.{Collection, Database}
import quasar.{Disposable, EffectfulQSpec}

import org.bson.{Document => _, _}
import org.mongodb.scala.{Completed, Document, MongoClient, MongoSecurityException, MongoTimeoutException}
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
      Mongo[IO](MongoConfig("http://localhost", 128, PushdownLevel.Disabled, None)).unsafeRunSync() must throwA[java.lang.IllegalArgumentException]
    }

    "for unreachable config" >> {
      Mongo[IO](MongoConfig("mongodb://unreachable", 128, PushdownLevel.Disabled, None)).unsafeRunSync() must throwA[MongoTimeoutException]
    }
  }

  "getting databases works correctly" >>* mkMongo.flatMap(_ { mongo =>
    mongo.databases.compile.toList.map { evaluatedDbs =>
      MongoSpec.correctDbs.toSet.subsetOf(evaluatedDbs.toSet)
    }
  })

  "getting databases for constrained role works correctly A" >>* mkAMongo.flatMap(_ { mongo =>
    mongo.databases.compile.toList.map { _ === List(Database("A")) }})

  "getting databases for constrained role works correctly B" >>* mkBMongo.flatMap(_ { mongo =>
    mongo.databases.compile.toList.map { _ === List(Database("B")) }})

  "it's impossible to make mongo with incorrect auth" >> {
    mkInvalidAMongo.unsafeRunSync() must throwA[MongoSecurityException]
  }

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
    s"checking ${db.name}" >>* mkMongo.flatMap(_ { mongo =>
      mongo.collections(db)
        .fold(List[Collection]())((lst, coll) => coll :: lst)
        .map(collectionList => collectionList.toSet === MongoSpec.cols.map(c => Collection(db, c)).toSet)
        .compile
        .lastOrError
    })
  )

  "collections returns correct collection lists for constrained roles B" >> {
    "B" >>* mkBMongo.flatMap(_ { mongo =>
      mongo.collections(Database("B")).compile.toList.map { _ === List() }})
    "B.b" >>* mkBBMongo.flatMap(_ { mongo =>
      mongo.collections(Database("B")).compile.toList.map { _ == List(Collection(Database("B"), "b")) }})
  }

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

  "evaluteImpl works falls back if there is an exception" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.flatMap(_ { mongo =>
      mongo.evaluateImpl(col, incorrectPipeline, mongo.findAll(col)).flatMap { case (_, stream) =>
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

  "findAll raises path not found for nonexistent collections" >> Fragment.foreach(MongoSpec.incorrectCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.flatMap(_ { mongo =>
      IO.delay(mongo.findAll(col).attempt.unsafeRunSync() must beLike {
        case Left(t) => ResourceError.throwableP.getOption(t) must_=== Some(ResourceError.pathNotFound(col.resourcePath))
      })
    })
  )

  "tunnels" >> {
    "via key identity" >>* keyTunneledMongo.map(_ must not(throwA[Throwable]))
    "via user/password pair" >>* passwordTunneledMongo.map(_ must not(throwA[Throwable]))
  }
}

object MongoSpec {
  import java.nio.charset.StandardCharsets
  import java.nio.file.{Files, Paths}

  import Mongo._
  import TunnelConfig.Credentials._

  val dbs = List("A", "B", "C", "D")
  val cols = List("a", "b", "c", "d")
  val nonexistentDbs = List("Z", "Y")
  val nonexistentCols = List("z", "y")

  val host = Source.fromResource("mongo-host").mkString.trim

  val port: String = "27018"
  val connectionString: String = s"mongodb://root:secret@${host}:${port}"
  val connectionStringInvalidPort: String = s"mongodb://root:secret@${host}:27000/?serverSelectionTimeoutMS=1000"
  val aConnectionString: String = s"mongodb://aUser:aPassword@${host}:${port}/A.a"
  val invalidAConnectionString: String = s"mongodb://aUser:aPassword@${host}:${port}"
  // Note, there is no collection, only db
  val bConnectionString: String = s"mongodb://bUser:bPassword@${host}:${port}/B"
  // And, there we have collection .b
  val bbConnectionString: String = s"mongodb://bUser:bPassword@${host}:${port}/B.b"

  val BatchSize: Int = 64

  def mkMongo: IO[Disposable[IO, Mongo[IO]]] =
    Mongo[IO](MongoConfig(connectionString, BatchSize, PushdownLevel.Full, None))

  def privateKey: IO[String] = IO.delay {
    val path = Paths.get("key_for_docker")
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
  }


  def keyTunneledMongo: IO[Disposable[IO, Mongo[IO]]] =
    Mongo[IO](MongoConfig(
      "mongodb://root:secret@localhost:27017",
      BatchSize,
      PushdownLevel.Full,
      Some(TunnelConfig("localhost", 22222,
        Some(LoginPassword("root", "root"))))))

  def passwordTunneledMongo: IO[Disposable[IO, Mongo[IO]]] = privateKey flatMap { key =>
    Mongo[IO](MongoConfig(
      "mongodb://root:secret@localhost:27017",
      BatchSize,
      PushdownLevel.Full,
      Some(TunnelConfig("localhost", 22222,
        Some(Identity(key, Some("passphrase")))))))
  }



  def mkAMongo: IO[Disposable[IO, Mongo[IO]]] =
    Mongo[IO](MongoConfig(aConnectionString, BatchSize, PushdownLevel.Full, None))

  def mkInvalidAMongo: IO[Disposable[IO, Mongo[IO]]] =
    Mongo[IO](MongoConfig(invalidAConnectionString, BatchSize, PushdownLevel.Full, None))

  // create an invalid Mongo to test error scenarios, bypassing the ping check that's done in Mongo.apply
  def mkMongoInvalidPort: IO[Disposable[IO, Mongo[IO]]] =
    for {
      clientDisposable <- Mongo.mkClient[IO](MongoConfig(connectionStringInvalidPort, 64, PushdownLevel.Full, None))
      interpreter = new Interpreter(Version(0, 0, 0), "redundant", PushdownLevel.Full)
    } yield clientDisposable flatMap { (c: MongoClient) =>
      val mongo = new Mongo[IO](c, BatchSize.toLong, PushdownLevel.Full, interpreter, None)
      Disposable(mongo, IO.unit)
    }


  def mkBMongo: IO[Disposable[IO, Mongo[IO]]] =
    Mongo[IO](MongoConfig(bConnectionString, BatchSize, PushdownLevel.Full, None))
  def mkBBMongo: IO[Disposable[IO, Mongo[IO]]] =
    Mongo[IO](MongoConfig(bbConnectionString, BatchSize, PushdownLevel.Full, None))


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

  val incorrectPipeline: List[BsonDocument] = List(new BsonDocument("$incorrect", new BsonInt32(0)))

  def setupDB(): Unit = {
    val ioSetup = for {
      client <- IO.delay(MongoClient(connectionString))
      _ <- correctCollections.traverse { col => for {
        mongoCollection <- IO.delay(client.getDatabase(col.database.name).getCollection(col.name))
        _ <- singleObservableAsF[IO, Completed](mongoCollection.drop).attempt
        _ <- singleObservableAsF[IO, Completed](mongoCollection.insertOne(Document(col.name -> col.database.name)))
        // aUser:aPassword --> read only access to only one db, this means that aUser can't run listDatabases
        aDatabase <- IO.delay(client.getDatabase("A"))
        _ <- singleObservableAsF[IO, Document](aDatabase.runCommand[Document](Document(
          "createUser" -> "aUser",
          "pwd" -> "aPassword",
          "roles" -> List(Document("role" -> "read", "db" -> "A"))))).attempt
        // bUser:bPassword --> can do anything with "B" roles, but can't do anything with data level listCollections
        bDatabase <- IO.delay(client.getDatabase("B"))
        _ <- singleObservableAsF[IO, Document](bDatabase.runCommand[Document](Document(
          "createUser" -> "bUser",
          "pwd" -> "bPassword",
          "roles" -> List(Document("role" -> "userAdmin", "db" -> "B"))))).attempt
        } yield ()
      }
      _ <- IO.delay(client.close())
    } yield ()
    ioSetup.unsafeRunSync()
  }
}
