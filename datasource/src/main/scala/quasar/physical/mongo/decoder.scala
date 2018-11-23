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
import qdata._
import qdata.QType._
import quasar.contrib.std.errorImpossible

import java.time._
import java.util.{Map, Iterator}

import eu.timepit.refined.auto._
import org.bson._
import spire.math.Real

object decoder {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  class BsonDocumentCursor(document: BsonDocument) {

    val iterator: Iterator[Map.Entry[String, BsonValue]] =
      document.entrySet().iterator()

    var entry: Option[Map.Entry[String, BsonValue]] = None

    def hasNext(): Boolean = {
      try {
        entry = Some(iterator.next())
        true
      } catch {
        case _: Throwable => false
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
      case BsonType.DECIMAL128 => QReal
      case BsonType.BOOLEAN => QBoolean
      case BsonType.OBJECT_ID => QString
      case BsonType.DB_POINTER => QString
      case BsonType.BINARY => QString
      case BsonType.DATE_TIME => QOffsetDateTime
      case BsonType.SYMBOL => QString
      case BsonType.REGULAR_EXPRESSION => QString
      case BsonType.JAVASCRIPT => QString
      case BsonType.NULL => QNull
      case BsonType.TIMESTAMP => QOffsetDateTime
      case BsonType.UNDEFINED => QNull
      case BsonType.JAVASCRIPT_WITH_SCOPE => QString
      case BsonType.MAX_KEY => QString
      case BsonType.MIN_KEY => QString
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
    override def getLong(bson: BsonValue): Long = bson match {
      case num: BsonNumber => num.longValue()
    }
    override def getReal(bson: BsonValue): Real = bson match {
      case num: BsonNumber => Real(num.decimal128Value().bigDecimalValue())
    }
    override def getString(bson: BsonValue): String = bson match {
      case str: BsonString => str.getValue()
      case objId: BsonObjectId => objId.getValue().toHexString()
      case pointer: BsonDbPointer => {
        pointer.getNamespace() ++ ":" ++ pointer.getId().toHexString()
      }
      case binary: BsonBinary => new String(binary.getData())
      case symbol: BsonSymbol => symbol.getSymbol()
      case regexp: BsonRegularExpression => regexp.getPattern()
      case js: BsonJavaScript => js.getCode()
      case maxKey: BsonMaxKey => maxKey.toString()
      case minKey: BsonMinKey => minKey.toString()
      case jsWithScope: BsonJavaScriptWithScope => jsWithScope.getCode()
    }

    override def getOffsetDateTime(bson: BsonValue): OffsetDateTime = bson match {
      case ts: BsonTimestamp => {
        Instant.ofEpochSecond(ts.getTime().longValue).atOffset(ZoneOffset.UTC)
      }
      case date: BsonDateTime => {
        Instant.ofEpochMilli(date.getValue()).atOffset(ZoneOffset.UTC)
      }
    }

    override def getInterval(a: BsonValue) = errorImpossible
    override def getLocalDate(a: BsonValue) = errorImpossible
    override def getLocalDateTime(a: BsonValue) = errorImpossible
    override def getLocalTime(a: BsonValue) = errorImpossible
    override def getMetaMeta(a: BsonValue) = errorImpossible
    override def getMetaValue(a: BsonValue) = errorImpossible
    override def getOffsetDate(a: BsonValue) = errorImpossible
    override def getOffsetTime(a: BsonValue) = errorImpossible
  }
}
