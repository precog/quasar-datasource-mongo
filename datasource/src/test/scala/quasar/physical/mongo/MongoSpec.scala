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

import cats.effect.{IO, Resource}
import cats.instances.list._
import cats.syntax.apply._
import cats.syntax.traverse._

import quasar.concurrent.BlockingContext
import quasar.connector.ResourceError
import quasar.physical.mongo.MongoResource.{Collection, Database}
import quasar.EffectfulQSpec

import org.bson.{Document => _, _}
import org.mongodb.scala.{Completed, Document, MongoSecurityException, MongoTimeoutException}
import org.specs2.specification.core._
import org.specs2.execute.AsResult
import scala.io.Source

import shims._
import testImplicits._

class MongoSpec extends EffectfulQSpec[IO] {
  import MongoSpec._
  step(MongoSpec.setupDB.unsafeRunSync())

  "can create client from valid connection string" >>* mkMongo.use(IO.pure).attempt.map(_ must beRight)

  "can't create client from incorrect connection string" >> {
    "for incorrect protocol" >> {
      Mongo[IO](
        MongoConfig.basic("http://localhost"),
        BlockingContext.cached("not-used"))
        .use(IO.pure)
        .unsafeRunSync() must throwA[java.lang.IllegalArgumentException]
    }

    "for unreachable config" >> {
      Mongo[IO](
        MongoConfig.basic("mongodb://unreachable"),
        BlockingContext.cached("not-used"))
        .use(IO.pure)
        .unsafeRunSync() must throwA[MongoTimeoutException]
    }
  }

  "getting databases works correctly" >>* mkMongo.use { mongo =>
    mongo.databases.compile.toList.map { evaluatedDbs =>
      MongoSpec.correctDbs.toSet.subsetOf(evaluatedDbs.toSet)
    }
  }

  "getting databases for constrained role works correctly A" >>* mkAMongo.use { mongo =>
    mongo.databases.compile.toList.map { _ === List(Database("A")) }
  }

  "getting databases for constrained role works correctly B" >>* mkBMongo.use { mongo =>
    mongo.databases.compile.toList.map { _ === List(Database("B")) }
  }

  "it's impossible to make mongo with incorrect auth" >> {
    mkInvalidAMongo.use(IO.pure).unsafeRunSync() must throwA[MongoSecurityException]
  }

  "databaseExists returns true for existing dbs" >> Fragment.foreach(MongoSpec.correctDbs)(db =>
      s"checking ${db.name}" >>* mkMongo.use { mongo =>
        mongo.databaseExists(db).compile.lastOrError
      }
  )

  "databaseExists returns false for non-existing dbs" >> Fragment.foreach(MongoSpec.incorrectDbs)(db =>
    s"checking ${db.name}" >>* mkMongo.use { mongo =>
      mongo.databaseExists(db).map(!_).compile.lastOrError
    }
  )

  "collections returns correct collection lists" >> Fragment.foreach(MongoSpec.correctDbs)(db =>
    s"checking ${db.name}" >>* mkMongo.use { mongo =>
      mongo.collections(db)
        .fold(List[Collection]())((lst, coll) => coll :: lst)
        .map(collectionList => collectionList.toSet === MongoSpec.cols.map(c => Collection(db, c)).toSet)
        .compile
        .lastOrError
    }
  )

  "collections returns correct collection lists for constrained roles B" >> {
    "B" >>* mkBMongo.use { mongo =>
      mongo.collections(Database("B")).compile.toList.map { _ === List() }
    }

    "B.b" >>* mkBBMongo.use { mongo =>
      mongo.collections(Database("B")).compile.toList.map { _ == List(Collection(Database("B"), "b")) }
    }
  }

  "collections return empty stream for non-existing databases" >> Fragment.foreach(MongoSpec.incorrectDbs)(db =>
    s"checking ${db.name}" >>* mkMongo.use { mongo =>
      mongo.collections(db).compile.toList.map(_ === List[Collection]())
    }
  )

  "collectionExists returns true for existent collections" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.use { mongo =>
      mongo.collectionExists(col).compile.lastOrError
    }
  )

  "collectionExists returns false for non-existent collections" >> Fragment.foreach(MongoSpec.incorrectCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.use { mongo =>
      mongo.collectionExists(col).map(!_).compile.lastOrError
    }
  )

  "findAll returns correct results for existing collections" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.use { mongo =>
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
    }
  )

  "evaluteImpl works falls back if there is an exception" >> Fragment.foreach(MongoSpec.correctCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.use { mongo =>
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
    }
  )

  "findAll raises path not found for nonexistent collections" >> Fragment.foreach(MongoSpec.incorrectCollections)(col =>
    s"checking ${col.database.name} :: ${col.name}" >>* mkMongo.use { mongo =>
      mongo.findAll(col).attempt.map(_ must beLike {
        case Left(t) => ResourceError.throwableP.getOption(t) must_=== Some(ResourceError.pathNotFound(col.resourcePath))
      })
    }
  )

  "ssl" >> {
    "can't connect w/o any ssl configuration" >>* {
      Mongo[IO](MongoConfig.basic(insecuredConnectionString), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beLeft)
    }

    "can connect when ssl enabled" >>* {
      val sslConfig = SSLConfig(None, None, true, None)
      Mongo[IO](MongoConfig.basic(securedConnectionString).useSSL(sslConfig), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beRight)
    }

    "can connect when ssl enabled via connectionString only" >>* {
      Mongo[IO](MongoConfig.basic(securedConnectionString), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beRight)
    }

    "can connect with certificate authority provided" >>* {
      val certString = """
-----BEGIN CERTIFICATE-----
MIIGFTCCA/2gAwIBAgIJAJjzUnLXxIlrMA0GCSqGSIb3DQEBCwUAMIGgMQswCQYD
VQQGEwJVUzELMAkGA1UECAwCQ08xITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMg
UHR5IEx0ZDExMC8GA1UECwwoVHJhdmlzIHRlc3RzIGZvciBxdWFzYXItZGF0YXNv
dXJjZS1tb25nbzENMAsGA1UEAwwEcm9vdDEfMB0GCSqGSIb3DQEJARYQYm90QHNs
YW1kYXRhLmNvbTAeFw0xOTA5MDIxMjMyMTZaFw0yMjA2MjIxMjMyMTZaMIGgMQsw
CQYDVQQGEwJVUzELMAkGA1UECAwCQ08xITAfBgNVBAoMGEludGVybmV0IFdpZGdp
dHMgUHR5IEx0ZDExMC8GA1UECwwoVHJhdmlzIHRlc3RzIGZvciBxdWFzYXItZGF0
YXNvdXJjZS1tb25nbzENMAsGA1UEAwwEcm9vdDEfMB0GCSqGSIb3DQEJARYQYm90
QHNsYW1kYXRhLmNvbTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAMkN
PLjWunQbKHJ93W+gT/UdpkYqp/N1bv8uxZiYhUjE+xm2dfRa8IJJRuxxx6RjJVhL
9WL4/pruGY0lrQPj4K/rqOWtGyzkLMQAvUBft0hgtVg9h/Eig7IiPQpyhBPQfu73
gSCIfviZX0jlcbC/HhhIO/wG185lMXG2Jpvztu6uFWISdoQn/WKnkzv6w9TcUTjS
ZEhiklXlRIj4S/65x7l0PJdjZM86BF1cWipHwcHwmkHJxVReZky0o5psRApLUBzy
aNKcmGBbLtdgdTQKTZhNhfYH1GxCqNopUeBE84/XvKx/8WFFf6LQ6e2lN1+qzM7U
0zg0iqYrYgyiHWWQJaIY0rksmfQWyaNimvNKRA1HfEzHW9aFmgkt2K4ncQ8BwNm7
dFPiToKQWCyK2vvYof/QUcb/f9JFlhcFaoHxdjvbV52pxnEEqBo8l9J94opRcx94
PuTByYYs+MLLO1vuqeilVAp+8bUfvQtsR/4Ri8vgd/Egy18nUPCDyLePj4WnuIKH
QEGQwzVR6nmkwQlPrDMPa0HGvQk5HevePcTErP98SjE6MkyzACO+a96A5w9QF8X/
TiNTXI28ieTbRvog9q+oSW1GBEEDVOMrBm+WubA5NpNLc9HGrBYFhi4XYzXRM7RX
BLeqChZrcOP/79Ym9J22MfHLmZH1hdkWKY1ralIRAgMBAAGjUDBOMB0GA1UdDgQW
BBSLuYAnqAU5BRzxjU5EDzymFejknjAfBgNVHSMEGDAWgBSLuYAnqAU5BRzxjU5E
DzymFejknjAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQBvGo6DgfNc
/dQJ0ROZVtW4TPa0Pa+t8SydtPS3533wdZmqlDZWNhRPvENVKyowMhyDCmmdcTpn
X9FOdVMHf9WMeH1BDedjrqHpcLWHnOk/RMh6kJ95Qg15rdMTFUmLPv4XbWhh1Sof
SNZB2QXMG9G9Lu8Vg1DRrKs8TCcuaCZfnKERA4CiL0G6DTLd1os6QxZcOdrI+0PR
+9zErCvxTgRjO8cVLvLnkjBScrsEVXRX7KhHJQAv/jHlZHCW44IpXicEHGHGZn0F
QAHMb92NEaF10+GbZAoXi0Q2emDP+R4YDBNjG+wk2gcHLAnxDjbrhtPRrjUkPpiV
GdiNNFkplIJnCZepHiGc1Lhc477lP+MRFjfBdjAksPn4ECd902JdjUI4wFqnIU/y
mRp+TykVv9NmFWkhbf+T1gzt4S1eaWZT5BLp4x3jimOQvkEFXYLkfVwWRT/e3oJp
TtZYRyUgo/772y6/WwUoC7JVtDrAP4c78EyjY3tB2D5F4ruDQWN2s48ziautg7cM
KogCnn7keHB3k1PxmM+y8L4FLB8QG+54z9uMO9Q2bSIjB1PPHmY3GKLqOljmN3pz
Il6xnf3gHCMdRqP+++aZhIwtWGECD57n5CoP25iPelSVp9CmBKuu8HGFTEajofFK
B8nj9xIMi13bXa9zlWQquzo5WofYVSJxjw==
-----END CERTIFICATE-----
"""
      val sslConfig = SSLConfig(Some(certString), None, true, None)
      Mongo[IO](MongoConfig.basic(securedConnectionString).useSSL(sslConfig), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beRight)
    }
    "can't connect with incorrect certificate" >>* {
      val sslConfig = SSLConfig(Some("incorrect certificate"), None, true, None)
      Mongo[IO](MongoConfig.basic(securedConnectionString).useSSL(sslConfig), blockingPool)
        .use(IO.pure)
        .attempt
        .map(_ must beLeft)
    }

  }
  "tunnels" >> {
    "via key identity" >>* keyTunneledMongo.use(IO.pure).attempt.map(_ must beRight)
    "via user/password pair" >>* passwordTunneledMongo.use(IO.pure).attempt.map(_ must beRight)
  }
}

object MongoSpec {
  import java.nio.file.{Files, Paths}

  import Mongo._
  import TunnelConfig.Pass._

  private lazy val blockingPool: BlockingContext = BlockingContext.cached("mongo-datasource")

  val dbs = List("A", "B", "C", "D")
  val cols = List("a", "b", "c", "d")
  val nonexistentDbs = List("Z", "Y")
  val nonexistentCols = List("z", "y")

  val host = Source.fromResource("mongo-host").mkString.trim

  val port: String = "27018"
  val securePort: String = "27019"

  val connectionString: String = s"mongodb://root:secret@${host}:${port}"
  val insecuredConnectionString: String = s"mongodb://${host}:${securePort}/?serverSelectionTimeoutMS=1000"
  val securedConnectionString: String = s"${insecuredConnectionString}&ssl=true"
  val connectionStringInvalidPort: String = s"mongodb://root:secret@${host}:27000/?serverSelectionTimeoutMS=1000"
  val aConnectionString: String = s"mongodb://aUser:aPassword@${host}:${port}/A.a"
  val invalidAConnectionString: String = s"mongodb://aUser:aPassword@${host}:${port}/"
  // Note, there is no collection, only db
  val bConnectionString: String = s"mongodb://bUser:bPassword@${host}:${port}/B"
  // And, there we have collection .b
  val bbConnectionString: String = s"mongodb://bUser:bPassword@${host}:${port}/B.b"

  val BatchSize: Int = 64

  def mkMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig.basic(connectionString).withPushdown(PushdownLevel.Full), blockingPool)

  def privateKey: IO[String] = IO.delay {
    val path = Paths.get("key_for_docker")
    new String(Files.readAllBytes(path))
  }

  val TunneledURL: String = "mongodb://root:secret@mng:27017"
  val TunnelUser: String = "root"
  val TunnelPassword: String = "root"
  val TunnelPassphrase: Option[String] = Some("passphrase")
  val TunnelHost: String = "localhost"
  val TunnelPort: Int = 22222

  def passwordTunneledMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig.basic(TunneledURL)
      .withPushdown(PushdownLevel.Full)
      .viaTunnel(TunnelConfig(TunnelHost, TunnelPort, TunnelUser, Some(Password(TunnelPassword)))),
      blockingPool)

  def keyTunneledMongo: Resource[IO, Mongo[IO]] =
    Resource.liftF(privateKey) flatMap { key =>
      Mongo[IO](MongoConfig.basic(TunneledURL)
        .withPushdown(PushdownLevel.Full)
        .viaTunnel(TunnelConfig(TunnelHost, TunnelPort, TunnelUser, Some(Identity(key, TunnelPassphrase)))),
        blockingPool)
    }

  def mkAMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig.basic(aConnectionString).withPushdown(PushdownLevel.Full), blockingPool)

  def mkInvalidAMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig.basic(invalidAConnectionString).withPushdown(PushdownLevel.Full), blockingPool)

  // create an invalid Mongo to test error scenarios, bypassing the ping check that's done in Mongo.apply
  def mkMongoInvalidPort: Resource[IO, Mongo[IO]] =
    for {
      client <- Mongo.mkClient[IO](
        MongoConfig(connectionStringInvalidPort, 64, PushdownLevel.Full, None, None),
        blockingPool)

      interpreter = new Interpreter(Version(0, 0, 0), "redundant", PushdownLevel.Full)
    } yield {
      new Mongo[IO](
        client, BatchSize.toLong, PushdownLevel.Full, interpreter, None)
    }

  def mkBMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig(bConnectionString, BatchSize, PushdownLevel.Full, None, None), blockingPool)

  def mkBBMongo: Resource[IO, Mongo[IO]] =
    Mongo[IO](MongoConfig(bbConnectionString, BatchSize, PushdownLevel.Full, None, None), blockingPool)

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

  def setupDB(): IO[Unit] = {
    val clientR =
      Mongo.mkClient[IO](MongoConfig(connectionString, BatchSize, PushdownLevel.Full, None, None), blockingPool)

    clientR.use(client => for {
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
    } yield ())
  }
}
