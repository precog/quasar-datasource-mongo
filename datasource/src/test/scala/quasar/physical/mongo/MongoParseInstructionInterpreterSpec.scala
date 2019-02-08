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

import quasar.common.{CPath, CPathField}
import quasar.contrib.std.errorImpossible
import quasar.{IdStatus, ScalarStageSpec => Spec, JsonSpec, ScalarStage, ScalarStages}, ScalarStage._

import qdata.QDataEncode
import qdata.json.QDataFacade
import qdata.time.{DateTimeInterval, OffsetDate}

import org.bson._
import org.bson.types.Decimal128
import org.specs2.mutable.Specification

import java.time.{
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime,
  Instant,
}


import org.typelevel.jawn.{AsyncParser, Facade}

import spire.math.Real

import shims._

class MongoParseInstructionInterpreterSpec
    extends JsonSpec
//    with Spec.WrapSpec
//    with Spec.ProjectSpec
//    with Spec.PivotSpec
    with Spec.MaskSpec
{
  import InterpreterSpec._
  import MongoSpec.mkMongo

  type JsonElement = BsonValue

  val RootKey: String = "rootKey"

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

  def interpret(stages: List[ScalarStage], inp: JsonStream): JsonStream = {
    mkMongo.flatMap(_ { mongo => for {
      c <- uniqueCollection
      _ <- dropCollection(c, mongo)
      _ <- insertValues(c, mongo, inp map { x => new BsonDocument(RootKey, x) })
      stream <- mongo.evaluate(c, ScalarStages(
        IdStatus.ExcludeId,
        (Project(CPath.parse(s".${RootKey}")) :: stages)))
//      _ <- IO.delay(scala.Predef.println(s"stages: ${stages}"))
//      _ <- IO.delay(scala.Predef.println(s"inserted: ${inp map { x => new BsonDocument(RootKey, x) }}"))
      actual <- stream._2.compile.toList
//      _ <- IO.delay(scala.Predef.println(s"actual: ${actual}"))
      _ <- dropCollection(c, mongo)
    } yield actual }).unsafeRunSync()
  }

  def evalWrap(wrap: Wrap, stream: JsonStream): JsonStream =
    interpret(List(wrap), stream)

  def evalProject(project: Project, stream: JsonStream): JsonStream =
    interpret(List(project), stream)

  def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream =
    interpret(List(pivot), stream)


  def evalMask(mask: Mask, stream: JsonStream): JsonStream =
    interpret(List(mask), stream)


  protected def ldjson(str: String): JsonStream = {
    implicit val facade: Facade[JsonElement] = QDataFacade[JsonElement](isPrecise = false)

    val parser: AsyncParser[JsonElement] =
      AsyncParser[JsonElement](AsyncParser.ValueStream)

    val events1: JsonStream = parser.absorb(str).right.get.toList
    val events2: JsonStream = parser.finish().right.get.toList

    events1 ::: events2
  }
}
