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

import java.time.{Instant, ZoneOffset}

import org.bson._
import org.bson.types.{Decimal128, ObjectId}
import org.specs2.mutable.Specification
import org.specs2.specification.core._

import qdata.QType, QType._

import scala.collection.JavaConverters._

import spire.math.Real

class DecodeSpec extends Specification {
  import decoder.qdataDecoder

  "decoder decodes bsons with correct types" >> {
    val large = new Decimal128((BigDecimal.decimal(Double.MaxValue) * BigDecimal.decimal(12.23)).bigDecimal)
    val tiny = new Decimal128((BigDecimal.decimal(Double.MinPositiveValue) * BigDecimal.decimal(0.0000001)).bigDecimal)
    val checks = List(
      new BsonNull() -> QNull,
      new BsonInt64(1L) -> QLong,
      new BsonInt32(12) ->  QLong,
      new BsonSymbol("symbol") -> QMeta,
      new BsonDouble(1.2) -> QDouble,
      new BsonString("string") -> QString,
      new BsonBinary(Array[Byte]()) -> QMeta,
      new BsonMinKey() -> QMeta,
      new BsonMaxKey() -> QMeta,
      new BsonBoolean(true) -> QBoolean,
      new BsonObjectId() -> QMeta,
      new BsonDateTime(1000) -> QOffsetDateTime,
      new BsonDocument() -> QObject,
      new BsonArray() -> QArray,
      new BsonDbPointer("db", new ObjectId()) -> QMeta,
      new BsonObjectId() -> QMeta,
      new BsonTimestamp(112L) -> QMeta,
      new BsonUndefined() -> QNull,
      new BsonDecimal128(new Decimal128(112L)) -> QLong,
      new BsonDecimal128(new Decimal128(BigDecimal.decimal(12.23).bigDecimal)) -> QDouble,
      new BsonDecimal128(large) -> QReal,
      new BsonDecimal128(tiny) -> QReal,
      new BsonJavaScript("val a = undefined") -> QMeta,
      new BsonRegularExpression("*") -> QMeta,
      new BsonJavaScriptWithScope("val a = undefined", new BsonDocument()) -> QMeta,
    )
    Fragment.foreach(checks) (_ match {
      case (bson: BsonValue, expected: QType) =>
        s"$bson :-> $expected" >> { qdataDecoder.tpe(bson) === expected }
    })
  }
  "QLong values are correct" >> {
    "for int32" >> { qdataDecoder.getLong(new BsonInt32(12)) === 12L }
    "for int64" >> { qdataDecoder.getLong(new BsonInt64(12L)) === 12L }
  }

  "QDouble values are correct" >> {
    qdataDecoder.getDouble(new BsonDouble(12.2)) === 12.2
  }

  "QReal values are correct" >> {
    qdataDecoder.getReal(new BsonDecimal128(new Decimal128(112L))) === Real(112L)
  }

  "QBoolean values are correct" >> {
    qdataDecoder.getBoolean(new BsonBoolean(true)) === true
    qdataDecoder.getBoolean(new BsonBoolean(false)) === false
  }

  "QOffsetDateTime values are correct" >> {
    qdataDecoder.getOffsetDateTime(new BsonDateTime(123)) ===
      Instant.ofEpochMilli(123).atOffset(ZoneOffset.UTC),
  }
  "QString values are correct" >> {
    qdataDecoder.getString(new BsonString("foo")) == "foo"
    qdataDecoder.getString(new BsonString("bar")) == "bar"
  }
  "QArray works for empty arrays" >> {
    val bsonArr = new BsonArray()
    val cursor = qdataDecoder.getArrayCursor(bsonArr)
    qdataDecoder.hasNextArray(cursor) must beFalse
  }

  "QArray hasNextArray and getArrayAt are idempotent" >> {
    val bsonArr = new BsonArray(List(new BsonString("foo")).asJava)
    val cursor = qdataDecoder.getArrayCursor(bsonArr)
    qdataDecoder.hasNextArray(cursor) must beTrue
    qdataDecoder.getArrayAt(cursor).asString().getValue() === "foo"
    qdataDecoder.hasNextArray(cursor) must beTrue
    qdataDecoder.getArrayAt(cursor).asString().getValue() === "foo"
    qdataDecoder.hasNextArray(cursor) must beTrue
    qdataDecoder.getArrayAt(cursor).asString().getValue() === "foo"
    val cursor1 = qdataDecoder.stepArray(cursor)
    qdataDecoder.hasNextArray(cursor1) must beFalse
  }

  "QArray works for nonempty arrays" >> {
    val bsonArr = new BsonArray(List(
      new BsonString("foo"),
      new BsonInt64(42L),
      new BsonNull()
    ).asJava)
    val cursor = qdataDecoder.getArrayCursor(bsonArr)
    qdataDecoder.hasNextArray(cursor) must beTrue
    qdataDecoder.getArrayAt(cursor).asString().getValue() === "foo"
    val cursor1 = qdataDecoder.stepArray(cursor)
    qdataDecoder.hasNextArray(cursor1) must beTrue
    qdataDecoder.getArrayAt(cursor1).asInt64().getValue() === 42L
    val cursor2 = qdataDecoder.stepArray(cursor1)
    qdataDecoder.hasNextArray(cursor2) must beTrue
    qdataDecoder.getArrayAt(cursor2).isNull() must beTrue
    val cursor3 = qdataDecoder.stepArray(cursor2)
    qdataDecoder.hasNextArray(cursor3) must beFalse
  }

  "QObject works for empty documents" >> {
    val bsonDoc = new BsonDocument()
    val cursor = qdataDecoder.getObjectCursor(bsonDoc)
    qdataDecoder.hasNextObject(cursor) must beFalse
  }

  "QObject hasNextObject and getX are idempotent" >> {
    val bsonDoc = new BsonDocument(List(new BsonElement("a", new BsonString("foo"))).asJava)
    val cursor = qdataDecoder.getObjectCursor(bsonDoc)
    qdataDecoder.hasNextObject(cursor) must beTrue
    qdataDecoder.getObjectKeyAt(cursor) === "a"
    qdataDecoder.getObjectValueAt(cursor).asString().getValue() === "foo"
    qdataDecoder.hasNextObject(cursor) must beTrue
    qdataDecoder.getObjectKeyAt(cursor) === "a"
    qdataDecoder.getObjectValueAt(cursor).asString().getValue() === "foo"
    qdataDecoder.hasNextObject(cursor) must beTrue
    qdataDecoder.getObjectKeyAt(cursor) === "a"
    qdataDecoder.getObjectValueAt(cursor).asString().getValue() === "foo"
    val cursor1 = qdataDecoder.stepObject(cursor)
    qdataDecoder.hasNextObject(cursor1) must beFalse
  }

  "QObject works for nonempty documents" >> {
    val bsonDoc = new BsonDocument(List(
      new BsonElement("a", new BsonString("foo")),
      new BsonElement("b", new BsonInt64(42L)),
      new BsonElement("c", new BsonArray()),
      new BsonElement("d", new BsonSymbol("symbol")),
      new BsonElement("e", new BsonNull())
    ).asJava)
    val cursor = qdataDecoder.getObjectCursor(bsonDoc)
    qdataDecoder.hasNextObject(cursor) must beTrue
    qdataDecoder.getObjectKeyAt(cursor) === "a"
    qdataDecoder.getObjectValueAt(cursor).asString().getValue() === "foo"
    val cursor1 = qdataDecoder.stepObject(cursor)
    qdataDecoder.hasNextObject(cursor1) must beTrue
    qdataDecoder.getObjectKeyAt(cursor1) === "b"
    qdataDecoder.getObjectValueAt(cursor1).asInt64().getValue() === 42L
    val cursor2 = qdataDecoder.stepObject(cursor1)
    qdataDecoder.hasNextObject(cursor2) must beTrue
    qdataDecoder.getObjectKeyAt(cursor2) === "c"
    qdataDecoder.getObjectValueAt(cursor2).asArray().getValues().toArray().length === 0
    val cursor3 = qdataDecoder.stepObject(cursor2)
    qdataDecoder.hasNextObject(cursor3) must beTrue
    qdataDecoder.getObjectKeyAt(cursor3) === "d"
    qdataDecoder.getObjectValueAt(cursor3).asSymbol().getSymbol() === "symbol"
    val cursor4 = qdataDecoder.stepObject(cursor3)
    qdataDecoder.hasNextObject(cursor4) must beTrue
    qdataDecoder.getObjectKeyAt(cursor4) === "e"
    qdataDecoder.getObjectValueAt(cursor4).isNull() must beTrue
    val cursor5 = qdataDecoder.stepObject(cursor4)
    qdataDecoder.hasNextObject(cursor5) must beFalse
  }

  "QMeta meta checks" >> {
    def getVal(meta: BsonValue, key: String): BsonValue =
      meta.asDocument().get(key)

    def getStringVal(meta: BsonValue, key: String): String =
      getVal(meta, key).asString().getValue()

    def getIntVal(meta: BsonValue, key: String): Int =
      getVal(meta, key).asInt32().getValue()

    "objectId" >> {
      val obj = new BsonObjectId()
      val meta = qdataDecoder.getMetaMeta(obj)
      val refined = qdataDecoder.getMetaValue(obj)
      meta must haveClass[BsonDocument]
      getStringVal(meta, "type") === "mongo:objectId"
      refined must haveClass[BsonString]
    }
    "dbPointer" >> {
      val obj = new BsonDbPointer("namespace", new ObjectId())
      val meta = qdataDecoder.getMetaMeta(obj)
      val refined = qdataDecoder.getMetaValue(obj)
      meta must haveClass[BsonDocument]
      getStringVal(meta, "type") === "mongo:dbPointer"
      getStringVal(meta, "namespace") === "namespace"
      refined must haveClass[BsonString]
    }
    "binary" >> {
      "common" >> {
        val obj = new BsonBinary("ABCDEFGH123".getBytes())
        val meta = qdataDecoder.getMetaMeta(obj)
        val refined = qdataDecoder.getMetaValue(obj)
        meta must haveClass[BsonDocument]
        getStringVal(meta, "type") === "mongo:binary"
        refined must haveClass[BsonString]
        refined.asString().getValue() === "QUJDREVGR0gxMjM="
      }
      "uuid" >> {
        val uuid = java.util.UUID.randomUUID()
        val obj = new BsonBinary(uuid)
        val meta = qdataDecoder.getMetaMeta(obj)
        val refined = qdataDecoder.getMetaValue(obj)
        meta must haveClass[BsonDocument]
        getStringVal(meta, "type") === "mongo:binary"
        refined must haveClass[BsonString]
        refined.asString().getValue() === uuid.toString()
      }
      "md5" >> {
        val sec = java.security.MessageDigest.getInstance("MD5")
        sec.update("lemon".getBytes())
        val bytes = sec.digest()
        val obj = new BsonBinary(BsonBinarySubType.MD5, bytes)
        val meta = qdataDecoder.getMetaMeta(obj)
        val refined = qdataDecoder.getMetaValue(obj)
        meta must haveClass[BsonDocument]
        getStringVal(meta, "type") === "mongo:binary"
        refined must haveClass[BsonString]
        refined.asString().getValue() === new String(bytes)
      }
    }
    "symbol" >> {
      val obj = new BsonSymbol("1234")
      val meta = qdataDecoder.getMetaMeta(obj)
      val refined = qdataDecoder.getMetaValue(obj)
      meta must haveClass[BsonDocument]
      getStringVal(meta, "type") === "mongo:symbol"
      refined must haveClass[BsonString]
      refined.asString().getValue() === "1234"
    }
    "regex" >> {
      val obj = new BsonRegularExpression("*+", "i")
      val meta = qdataDecoder.getMetaMeta(obj)
      val refined = qdataDecoder.getMetaValue(obj)
      meta must haveClass[BsonDocument]
      getStringVal(meta, "type") === "mongo:regex"
      getStringVal(meta, "options") === "i"
      refined must haveClass[BsonString]
      refined.asString().getValue === "*+"
    }
    "javascript" >> {
      val obj = new BsonJavaScript("undefined")
      val meta = qdataDecoder.getMetaMeta(obj)
      val refined = qdataDecoder.getMetaValue(obj)
      meta must haveClass[BsonDocument]
      getStringVal(meta, "type") === "mongo:javascript"
      refined must haveClass[BsonString]
      refined.asString().getValue() === "undefined"
    }
    "timestamp" >> {
      val obj = new BsonTimestamp(0, 42)
      val meta = qdataDecoder.getMetaMeta(obj)
      val refined = qdataDecoder.getMetaValue(obj)
      meta must haveClass[BsonDocument]
      getStringVal(meta, "type") === "mongo:timestamp"
      getIntVal(meta, "inc") === 42
      refined must haveClass[BsonDateTime]
      refined.asDateTime().getValue() === 0L
    }
    "javascriptWithScope" >> {
      val obj = new BsonJavaScriptWithScope("x.a", new BsonDocument("x", new BsonInt32(42)))
      val meta = qdataDecoder.getMetaMeta(obj)
      val refined = qdataDecoder.getMetaValue(obj)
      meta must haveClass[BsonDocument]
      getStringVal(meta, "type") === "mongo:javascriptWithScope"
      val scope = getVal(meta, "scope")
      getIntVal(scope, "x") === 42
      refined must haveClass[BsonString]
      refined.asString().getValue() === "x.a"
    }
    "maxKey" >> {
      val obj = new BsonMaxKey()
      val meta = qdataDecoder.getMetaMeta(obj)
      val refined = qdataDecoder.getMetaValue(obj)
      meta must haveClass[BsonDocument]
      getStringVal(meta, "type") === "mongo:maxKey"
      refined must haveClass[BsonString]
      refined.asString().getValue() === "maxKey"
    }
    "minKey" >> {
      val obj = new BsonMinKey()
      val meta = qdataDecoder.getMetaMeta(obj)
      val refined = qdataDecoder.getMetaValue(obj)
      meta must haveClass[BsonDocument]
      getStringVal(meta, "type") === "mongo:minKey"
      refined must haveClass[BsonString]
      refined.asString().getValue() === "minKey"
    }
  }
  "BsonDecimal128 very special cases" >> {
    val nan =
      new BsonDecimal128(Decimal128.NaN)
    val negativeNaN =
      new BsonDecimal128(Decimal128.NEGATIVE_NaN)
    val inf =
      new BsonDecimal128(Decimal128.POSITIVE_INFINITY)
    val negativeInf =
      new BsonDecimal128(Decimal128.NEGATIVE_INFINITY)
    val zero =
      new BsonDecimal128(Decimal128.POSITIVE_ZERO)
    val negativeZero =
      new BsonDecimal128(Decimal128.NEGATIVE_ZERO)
    val maxLong =
      new BsonDecimal128(new Decimal128(Long.MaxValue))
    val minLong =
      new BsonDecimal128(new Decimal128(Long.MinValue))
    val maxDouble =
      new BsonDecimal128(new Decimal128(BigDecimal(Double.MaxValue).bigDecimal))
    val minDouble =
      new BsonDecimal128(new Decimal128(BigDecimal(Double.MinValue).bigDecimal))
    val tinyDouble =
      new BsonDecimal128(new Decimal128(
        BigDecimal(Double.MinPositiveValue).bigDecimal
      ))
    val moreThanMaxLong =
      new BsonDecimal128(new Decimal128((BigDecimal(Long.MaxValue) + BigDecimal(1L)).bigDecimal))
    val lessThanMinLong =
      new BsonDecimal128(new Decimal128((BigDecimal(Long.MinValue) - BigDecimal(1L)).bigDecimal))
    val moreThanMaxDouble =
      new BsonDecimal128(new Decimal128(
        (BigDecimal(Double.MaxValue) * 1.1).bigDecimal
      ))
    val lessThanMinDouble =
      new BsonDecimal128(new Decimal128(
        (BigDecimal(Double.MinValue) * 1.1).bigDecimal
      ))
    val tinierThanTiny =
      new BsonDecimal128(new Decimal128(
        (BigDecimal(Double.MinPositiveValue) / 2).bigDecimal
      ))
    val rational =
      new BsonDecimal128(new Decimal128(
        (BigDecimal(1L) / BigDecimal(3L)).bigDecimal
      ))
    val doubleRational =
      new BsonDecimal128(new Decimal128(
        BigDecimal(1.0 / 3.0).bigDecimal
      ))

    "tpe" >> {
      "nan" >> (qdataDecoder.tpe(nan) === QNull)
      "-nan" >> (qdataDecoder.tpe(negativeNaN) === QNull)
      "inf" >> (qdataDecoder.tpe(inf) === QNull)
      "-inf" >> (qdataDecoder.tpe(negativeInf) === QNull)
      "0" >> (qdataDecoder.tpe(zero) === QLong)
      "---0" >> (qdataDecoder.tpe(negativeZero) === QLong)
      "maxlong" >> (qdataDecoder.tpe(maxLong) === QLong)
      "minlong" >> (qdataDecoder.tpe(minLong) === QLong)
      "maxdouble" >> (qdataDecoder.tpe(maxDouble) === QDouble)
      "mindouble" >> (qdataDecoder.tpe(minDouble) === QDouble)
      "tinydouble" >> (qdataDecoder.tpe(tinyDouble) === QDouble)
      ">maxlong" >> (qdataDecoder.tpe(moreThanMaxLong) === QReal)
      "<minLong" >> (qdataDecoder.tpe(lessThanMinLong) === QReal)
      ">maxdouble" >> (qdataDecoder.tpe(moreThanMaxDouble) === QReal)
      "<mindouble" >> (qdataDecoder.tpe(lessThanMinDouble) === QReal)
      "abs < tinydouble" >> (qdataDecoder.tpe(tinierThanTiny) === QReal)
      "rational" >> (qdataDecoder.tpe(rational) === QReal)
      "doublerational" >> (qdataDecoder.tpe(doubleRational) === QDouble)
    }

    "getValue" >> {
      "0" >> (qdataDecoder.getLong(zero) === 0L)
      "---0" >> (qdataDecoder.getLong(negativeZero) === 0L)
      "maxlong" >> (qdataDecoder.getLong(maxLong) === Long.MaxValue)
      "minlong" >> (qdataDecoder.getLong(minLong) === Long.MinValue)
      "maxdouble" >> (qdataDecoder.getDouble(maxDouble) === Double.MaxValue)
      "mindouble" >> (qdataDecoder.getDouble(minDouble) === Double.MinValue)
      "tinydouble" >> (qdataDecoder.getDouble(tinyDouble) === Double.MinPositiveValue)
      ">maxlong" >> (qdataDecoder.getReal(moreThanMaxLong) === Real(Long.MaxValue) + 1.0)
      "<minlong" >> (qdataDecoder.getReal(lessThanMinLong) === Real(Long.MinValue) - 1.0)
      ">maxdouble" >> (qdataDecoder.getReal(moreThanMaxDouble) === Real(BigDecimal(Double.MaxValue) * 1.1))
      "<mindouble" >> (qdataDecoder.getReal(lessThanMinDouble) === Real(BigDecimal(Double.MinValue) * 1.1))
      "abs < tinydouble" >> (qdataDecoder.getReal(tinierThanTiny) === Real(BigDecimal(Double.MinPositiveValue) / 2))
      "rational" >> (qdataDecoder.getReal(rational) === Real(BigDecimal(1L) / BigDecimal(3L)))
      "doublerational" >> (qdataDecoder.getDouble(doubleRational) === 1.0 / 3.0)
    }
  }
}
