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

import org.bson._
import org.bson.types.{Decimal128, ObjectId}
import org.specs2.execute.{AsResult, Result}
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

import qdata.QType, QType._

import scala.collection.JavaConverters._

class DecodeProps extends Specification with ScalaCheck {
  import BsonValueGen._
  import decoder.qdataDecoder

  "every BsonValue have qtype" >> prop { bsonValue: BsonValue =>
    qdataDecoder.tpe(bsonValue) must not(throwA[Throwable])
  }.set(minTestsOk = 1000)

  "specifically all BsonDecimal128 have qtype" >> prop { bsonDecimal: BsonDecimal128 =>
    qdataDecoder.tpe(bsonDecimal) must not(throwA[Throwable])
  }.set(minTestsOk = 1000)

  private def checkBsonValue(bsonValue: BsonValue): Result = qdataDecoder.tpe(bsonValue) match {
    case QLong => qdataDecoder.getLong(bsonValue) must not(throwA[Throwable])
    case QDouble => qdataDecoder.getDouble(bsonValue) must not(throwA[Throwable])
    case QReal => qdataDecoder.getReal(bsonValue) must not(throwA[Throwable])
    case QString => qdataDecoder.getString(bsonValue) must not(throwA[Throwable])
    case QNull => AsResult(true)
    case QBoolean => qdataDecoder.getBoolean(bsonValue) must not(throwA[Throwable])
    case QLocalDateTime => AsResult(false).updateMessage("LocalDateTime is impossible in mongo")
    case QLocalDate => AsResult(false).updateMessage("LocalDate is impossible in mongo")
    case QLocalTime => AsResult(false).updateMessage("LocalTime is impossible in mongo")
    case QOffsetTime => AsResult(false).updateMessage("OffsetTime is impossible in mongo")
    case QOffsetDate => AsResult(false).updateMessage("OffsetDate is impossible in mongo")
    case QInterval => AsResult(false).updateMessage("Interval is impossible in mongo")
    case QOffsetDateTime => qdataDecoder.getOffsetDateTime(bsonValue) must not(throwA[Throwable])
    case QArray => qdataDecoder.getArrayCursor(bsonValue) must not (throwA[Throwable])
    case QObject => qdataDecoder.getObjectCursor(bsonValue) must not (throwA[Throwable])
    case QMeta => {
      qdataDecoder.getMetaMeta(bsonValue) must not(throwA[Throwable])
      qdataDecoder.getMetaValue(bsonValue) must not(throwA[Throwable])
      checkBsonValue(qdataDecoder.getMetaMeta(bsonValue))
      checkBsonValue(qdataDecoder.getMetaValue(bsonValue))
    }
  }

  "if we have qtype then we can get a value" >> prop { bsonValue: BsonValue =>
    checkBsonValue(bsonValue)
  }.set(minTestsOk = 1000)

  "BsonDecimal128 splitting works" >> prop { bsonDecimal: BsonDecimal128 =>
    val decimal128 = bsonDecimal.getValue()
    qdataDecoder.tpe(bsonDecimal) match {
      case QLong => AsResult(true)
      case QDouble => AsResult(true)
      case QNull =>
        AsResult(decimal128.isNaN() || decimal128.isInfinite())
          .updateMessage("BsonDecimal128 should be decoded to QNull iff it's either NaN or Infinite")
      case QReal => {
        val bdv = decimal128.bigDecimalValue()
        val moreThanMax = AsResult(bdv.compareTo(BigDecimal(Double.MaxValue).bigDecimal) > 0)
        val lessThanMin = AsResult(bdv.compareTo(BigDecimal(Double.MinValue).bigDecimal) < 0)
        val isTiny = AsResult(bdv.abs().compareTo(BigDecimal(Double.MinPositiveValue).bigDecimal) < 0)

        moreThanMax or lessThanMin or isTiny
      }
      case _ => AsResult(false).updateMessage("BsonDecimal128 should be decoded to number")
    }
  }.set(minTestsOk = 10000)
}

object BsonValueGen {
  import org.scalacheck._
  import Arbitrary._

  implicit def arbitraryBsonValue: Arbitrary[BsonValue] = Arbitrary(Gen.lzy(Gen.oneOf(
    genUnary,
    arbitrary[BsonInt32],
    arbitrary[BsonInt64],
    arbitrary[BsonBoolean],
    arbitrary[BsonDouble],
    arbitrary[BsonString],
    arbitrary[BsonDecimal128],
    arbitrary[BsonDbPointer],
    arbitrary[BsonDateTime],
    arbitrary[BsonBinary],
    arbitrary[BsonSymbol],
    arbitrary[BsonRegularExpression],
    arbitrary[BsonJavaScript],
    arbitrary[BsonJavaScriptWithScope],
    arbitrary[BsonArray],
    arbitrary[BsonDocument],
    arbitrary[BsonTimestamp]
  )))

  private val genUnary: Gen[BsonValue] =
    Gen.oneOf(
      Gen.const(new BsonNull()),
      Gen.const(new BsonUndefined()),
      Gen.const(new BsonMaxKey()),
      Gen.const(new BsonMinKey()),
      Gen.const(new BsonObjectId())
    )

  implicit val arbitraryBsonInt32: Arbitrary[BsonInt32] =
    Arbitrary(arbitrary[Int].map(new BsonInt32(_)))

  implicit val arbitraryBsonInt64: Arbitrary[BsonInt64] =
    Arbitrary(arbitrary[Long].map(new BsonInt64(_)))

  implicit val arbitraryBsonBoolean: Arbitrary[BsonBoolean] =
    Arbitrary(arbitrary[Boolean].map(new BsonBoolean(_)))

  implicit val arbitraryBsonString: Arbitrary[BsonString] =
    Arbitrary(Gen.alphaStr.map(new BsonString(_)))

  implicit val arbitraryBsonDouble: Arbitrary[BsonDouble] =
    Arbitrary(arbitrary[Double].map(new BsonDouble(_)))

  implicit val arbitraryBsonDecimal128: Arbitrary[BsonDecimal128] = Arbitrary(for {
    high <- arbitrary[Long]
    low <- arbitrary[Long]
    dec = Decimal128.fromIEEE754BIDEncoding(high, low)
  } yield new BsonDecimal128(dec))

  implicit val arbitraryBsonDbPointer: Arbitrary[BsonDbPointer] =
    Arbitrary(Gen.alphaStr.map(new BsonDbPointer(_, new ObjectId())))

  implicit val arbitraryBsonBinary: Arbitrary[BsonBinary] =
    Arbitrary(arbitrary[Array[Byte]].map(new BsonBinary(_)))

  implicit val arbitraryBsonDateTime: Arbitrary[BsonDateTime] =
    Arbitrary(arbitrary[Long].map(new BsonDateTime(_)))

  implicit val aribtraryBsonSymbol: Arbitrary[BsonSymbol] =
    Arbitrary(Gen.alphaStr.map(new BsonSymbol(_)))

  implicit val arbitraryBsonRegualrExpression: Arbitrary[BsonRegularExpression] =
    Arbitrary(Gen.alphaStr.map(new BsonRegularExpression(_)))

  implicit val arbitraryBsonJavascript: Arbitrary[BsonJavaScript] =
    Arbitrary(Gen.alphaStr.map(new BsonJavaScript(_)))

  implicit val arbitraryBsonTimestamp: Arbitrary[BsonTimestamp] = Arbitrary(for {
    time <- arbitrary[Int]
    inc <- arbitrary[Int]
  } yield new BsonTimestamp(time, inc))

  implicit val arbitraryBsonArray: Arbitrary[BsonArray] = {
    def go(n: Int, gen: Gen[BsonValue]): Gen[BsonValue] = if (n < 1) {
      genUnary
    } else {
      Gen.resize(n, Gen.listOf(go(n - 1, gen))).map(x => new BsonArray(x.asJava))
    }
    Arbitrary(go(6, arbitrary[BsonValue]).map(_.asArray()))
  }

  implicit val arbitraryBsonDocument: Arbitrary[BsonDocument] = {
    def go(n: Int, gen: Gen[BsonValue]): Gen[BsonValue] = if (n < 1) {
      genUnary
    } else {
      val element = for {
        key <- Gen.alphaStr
        value <- go(n - 1, gen)
      } yield new BsonElement(key, value)
      Gen.resize(n, Gen.listOf(element).map(x => new BsonDocument(x.asJava)))
    }
    Arbitrary(go(6, arbitrary[BsonValue]).map(_.asDocument()))
  }

  implicit val arbitraryBsonJavascriptWithScope: Arbitrary[BsonJavaScriptWithScope] = Arbitrary(for {
    js <- arbitrary[String]
    doc <- arbitrary[BsonDocument]
  } yield new BsonJavaScriptWithScope(js, doc))
}
