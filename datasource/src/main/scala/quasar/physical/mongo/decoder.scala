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

import java.lang.ArithmeticException
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.{Map, Iterator}

import org.bson._
import org.bson.types.Decimal128

import qdata.{QType, QDataDecode}, QType._

import quasar.contrib.std.errorImpossible

import scala.collection.JavaConverters._

import spire.math.Real

object decoder {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  class BsonDocumentCursor(document: BsonDocument) {

    val iterator: Iterator[Map.Entry[String, BsonValue]] =
      document.entrySet().iterator()

    var entry: Option[Map.Entry[String, BsonValue]] = None

    def hasNext(): Boolean = {
      if (iterator.hasNext()) {
        entry = Some(iterator.next())
        true
      } else {
        false
      }
    }

    def stepObject(): BsonDocumentCursor = this

    def getObjectKeyAt(): String = entry.getOrElse(errorImpossible).getKey()

    def getObjectValueAt(): BsonValue = entry.getOrElse(errorImpossible).getValue()

  }


  val qdataDecoder: QDataDecode[BsonValue] = new QDataDecode[BsonValue] {
    override def tpe(bson: BsonValue): QType = bson.getBsonType() match {
      case BsonType.DOCUMENT => QObject
      case BsonType.ARRAY => QArray
      case BsonType.STRING => QString
      case BsonType.INT32 => QLong
      case BsonType.INT64 => QLong
      case BsonType.DOUBLE => QDouble
      case BsonType.DECIMAL128 => {
        val dec128: Decimal128 = bson.asDecimal128().getValue()
        if (dec128.isNaN()) QNull
        else if (dec128.isInfinite()) QNull
        else try {
          val decimal: BigDecimal = BigDecimal(dec128.bigDecimalValue())
          if (decimal.isValidLong) QLong
          else if (decimal.isDecimalDouble) QDouble
          else QReal
        } catch {
          case e: ArithmeticException => QLong
        }
      }

      case BsonType.BOOLEAN => QBoolean
      case BsonType.OBJECT_ID => QMeta
      case BsonType.DB_POINTER => QMeta
      case BsonType.BINARY => QMeta
      case BsonType.DATE_TIME => QOffsetDateTime
      case BsonType.SYMBOL => QMeta
      case BsonType.REGULAR_EXPRESSION => QMeta
      case BsonType.JAVASCRIPT => QMeta
      case BsonType.NULL => QNull
      case BsonType.TIMESTAMP => QMeta
      case BsonType.UNDEFINED => QNull
      case BsonType.JAVASCRIPT_WITH_SCOPE => QMeta
      case BsonType.MAX_KEY => QMeta
      case BsonType.MIN_KEY => QMeta
      case BsonType.END_OF_DOCUMENT => QNull
    }

    type ArrayCursor = Iterator[BsonValue]

    override def getArrayCursor(bson: BsonValue): ArrayCursor = bson match {
      case arr: BsonArray => arr.getValues().iterator()
    }
    override def getArrayAt(cursor: ArrayCursor): BsonValue =
      cursor.next()
    override def hasNextArray(cursor: ArrayCursor): Boolean =
      cursor.hasNext()
    override def stepArray(cursor: ArrayCursor): ArrayCursor =
      cursor

    type ObjectCursor = BsonDocumentCursor

    override def getObjectCursor(bson: BsonValue): ObjectCursor = bson match {
      case obj: BsonDocument => new BsonDocumentCursor(obj)
    }
    override def getObjectKeyAt(cursor: ObjectCursor): String =
      cursor.getObjectKeyAt()
    override def getObjectValueAt(cursor: ObjectCursor): BsonValue =
      cursor.getObjectValueAt()
    override def hasNextObject(cursor: ObjectCursor): Boolean =
      cursor.hasNext()
    override def stepObject(cursor: ObjectCursor): ObjectCursor =
      cursor.stepObject()

    override def getBoolean(bson: BsonValue): Boolean = bson match {
      case bool: BsonBoolean => bool.getValue()
    }
    override def getDouble(bson: BsonValue): Double = bson match {
      case num: BsonNumber => num.doubleValue()
    }
    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    override def getLong(bson: BsonValue): Long = bson match {
      case bsonDecimal: BsonDecimal128 =>
        try {
          bsonDecimal.longValue()
        } catch {
          case e: ArithmeticException => 0L
        }
      case num: BsonNumber => num.longValue()
    }
    override def getReal(bson: BsonValue): Real = bson match {
      case num: BsonNumber => Real(num.decimal128Value().bigDecimalValue())
    }
    override def getString(bson: BsonValue): String = bson match {
      case str: BsonString => str.getValue()
    }
    override def getOffsetDateTime(bson: BsonValue): OffsetDateTime = bson match {
      case date: BsonDateTime => {
        Instant.ofEpochMilli(date.getValue()).atOffset(ZoneOffset.UTC)
      }
    }

    override def getMetaMeta(bson: BsonValue): BsonValue = bson match {
      case _: BsonObjectId => new BsonDocument("type", new BsonString("mongo:objectId"))
      case dbPointer: BsonDbPointer => new BsonDocument(List(
        new BsonElement("type", new BsonString("mongo:dbPointer")),
        new BsonElement("namespace", new BsonString(dbPointer.getNamespace())),
      ).asJava)
      case _: BsonBinary => new BsonDocument("type", new BsonString("mongo:binary"))
      case _: BsonSymbol => new BsonDocument("type", new BsonString("mongo:symbol"))
      case regex: BsonRegularExpression => new BsonDocument(List(
        new BsonElement("type", new BsonString("mongo:regex")),
        new BsonElement("options", new BsonString(regex.getOptions()))
      ).asJava)
      case _: BsonJavaScript => new BsonDocument("type", new BsonString("mongo:javascript"))
      case ts: BsonTimestamp => new BsonDocument(List(
        new BsonElement("type", new BsonString("mongo:timestamp")),
        new BsonElement("inc", new BsonInt32(ts.getInc()))
      ).asJava)
      case js: BsonJavaScriptWithScope => new BsonDocument(List(
        new BsonElement("type", new BsonString("mongo:javascriptWithScope")),
        new BsonElement("scope", js.getScope())
      ).asJava)
      case _: BsonMaxKey => new BsonDocument("type", new BsonString("mongo:maxKey"))
      case _: BsonMinKey => new BsonDocument("type", new BsonString("mongo:minKey"))
    }

    override def getMetaValue(bson: BsonValue): BsonValue = bson match {
      case objId: BsonObjectId => new BsonString(objId.getValue().toHexString())
      case dbPointer: BsonDbPointer => new BsonString(dbPointer.getId().toHexString())
      case binary: BsonBinary => new BsonString(new String(binary.getData()))
      case symbol: BsonSymbol => new BsonString(symbol.getSymbol())
      case regex: BsonRegularExpression => new BsonString(regex.getPattern())
      case js: BsonJavaScript => new BsonString(js.getCode())
      case ts: BsonTimestamp => new BsonDateTime(ts.getTime() * 1000L)
      case js: BsonJavaScriptWithScope => new BsonString(js.getCode())
      case maxKey: BsonMaxKey => new BsonString("maxKey")
      case minKey: BsonMinKey => new BsonString("minKey")
    }

    override def getInterval(a: BsonValue) = errorImpossible
    override def getLocalDate(a: BsonValue) = errorImpossible
    override def getLocalDateTime(a: BsonValue) = errorImpossible
    override def getLocalTime(a: BsonValue) = errorImpossible
    override def getOffsetDate(a: BsonValue) = errorImpossible
    override def getOffsetTime(a: BsonValue) = errorImpossible
  }
}
