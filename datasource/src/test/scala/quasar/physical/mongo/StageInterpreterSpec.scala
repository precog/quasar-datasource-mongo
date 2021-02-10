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

import cats.effect.IO
import cats.implicits._

import org.bson.types.Decimal128
import org.bson.{Document => _, _}
import org.mongodb.scala._
import org.mongodb.scala.result.InsertManyResult
import org.typelevel.jawn.{AsyncParser, Facade}

import quasar.contrib.std.errorImpossible
import quasar.physical.mongo.contrib.fs2.StreamSubscriber
import quasar.{JsonSpec, ScalarStages}

import qdata.QDataEncode
import qdata.json.QDataFacade
import qdata.time.{DateTimeInterval, OffsetDate}

import java.time.{
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime
}

import spire.math.Real

import shims._
import testImplicits._

import java.lang.Void

trait StageInterpreterSpec extends JsonSpec {
  import MongoSpec.mkMongo

  import MongoResource._

  type JsonElement = BsonValue

  def mkCollection(name: String): Collection = Collection(Database("aggregation_test"), name)

  def dropCollection(collection: Collection, mongo: Mongo[IO]): IO[Unit] =
    StreamSubscriber
      .fromPublisher[IO, Void](mongo.getCollection(collection).drop())
      .compile
      .lastOrError
      .attempt
      .void

  def insertValues(collection: Collection, mongo: Mongo[IO], vals: List[BsonValue]): IO[Unit] = {
    val docs: List[Document] = vals mapFilter {
      case (d: BsonDocument) => Some(Document(d))
      case _ => None
    }

    StreamSubscriber
      .fromPublisher[IO, InsertManyResult](
        mongo.getCollection(collection).insertMany(docs))
      .compile
      .lastOrError
      .void
  }

  val uniqueCollection: IO[Collection] =
    IO.delay(java.util.UUID.randomUUID().toString) map mkCollection

  implicit def bsonValueQDataEncode: QDataEncode[BsonValue] = new QDataEncode[BsonValue] {
    def makeLong(l: Long): BsonValue = new BsonInt64(l)
    def makeDouble(d: Double): BsonValue = new BsonDouble(d)
    def makeReal(r: Real): BsonValue = {
      val bd = r.toRational.toBigDecimal(java.math.MathContext.DECIMAL128)
      val dec128 = new Decimal128(bd.bigDecimal)
      new BsonDecimal128(dec128)
    }
    def makeString(s: String): BsonValue = new BsonString(s)
    def makeNull: BsonValue = new BsonNull()
    def makeBoolean(b: Boolean): BsonValue = new BsonBoolean(b)
    def makeLocalDateTime(l: LocalDateTime) = errorImpossible
    def makeLocalDate(l: LocalDate): BsonValue = errorImpossible
    def makeLocalTime(l: LocalTime): BsonValue = errorImpossible
    def makeOffsetDate(o: OffsetDate): BsonValue = errorImpossible
    def makeOffsetTime(o: OffsetTime): BsonValue = errorImpossible
    def makeOffsetDateTime(o: OffsetDateTime): BsonValue = new BsonDateTime(o.toZonedDateTime().toInstant().toEpochMilli())
    def makeInterval(i: DateTimeInterval): BsonValue = errorImpossible
    def makeMeta(a: BsonValue, b: BsonValue): BsonValue = errorImpossible

    type NascentArray = BsonArray

    def prepArray = new BsonArray()
    def pushArray(a: BsonValue, arr: BsonArray): BsonArray = {
      arr.add(a)
      arr
    }
    def makeArray(a: BsonArray): BsonValue = a

    type NascentObject = BsonDocument

    def prepObject: BsonDocument = new BsonDocument()
    def pushObject(key: String, a: BsonValue, obj: BsonDocument): BsonDocument = {
      obj.put(key, a)
      obj
    }
    def makeObject(a: BsonDocument): BsonValue = a
  }

  def interpret(stages: ScalarStages, inp: JsonStream, mapper: (BsonValue => BsonValue)): JsonStream =
    interpretWithOffset(stages, inp, None, mapper)

  def interpretWithOffset(stages: ScalarStages, inp: JsonStream, offset: Option[MongoOffset], mapper: (BsonValue => BsonValue))
      : JsonStream = {
    mkMongo.use(mongo => for {
      c <- uniqueCollection
      _ <- dropCollection(c, mongo)
      _ <- insertValues(c, mongo, inp map mapper)
      (_, stream) <- mongo.evaluate(c, stages, offset)
      actual <- stream.compile.toList
      _ <- dropCollection(c, mongo)
    } yield actual).unsafeRunSync
  }

  protected def ldjson(str: String): JsonStream = {
    implicit val facade: Facade[JsonElement] = QDataFacade[JsonElement](isPrecise = true)

    val parser: AsyncParser[JsonElement] =
      AsyncParser[JsonElement](AsyncParser.ValueStream)

    val events1: JsonStream = parser.absorb(str).right.get.toList
    val events2: JsonStream = parser.finish().right.get.toList

    events1 ::: events2
  }
}
