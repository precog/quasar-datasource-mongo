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
import quasar.physical.mongo.decoder.qdataDecoder
import qdata.QType._

import cats.effect.IO
import java.time._
import scala.collection.JavaConverters._

import eu.timepit.refined.auto._
import org.bson._
import org.bson.types.{Decimal128, ObjectId}
import org.specs2.mutable.Specification
import spire.math.Real
import fs2.Stream
import testImplicits._

class DecodeSpec extends Specification {
  "decoder decodes bsons with correct types" >> {
    List(
      qdataDecoder.tpe(new BsonNull()) ==== QNull,
      qdataDecoder.tpe(new BsonInt64(1L)) === QLong,
      qdataDecoder.tpe(new BsonInt32(12)) ===  QLong,
      qdataDecoder.tpe(new BsonSymbol("symbol")) === QMeta,
      qdataDecoder.tpe(new BsonDouble(1.2)) === QDouble,
      qdataDecoder.tpe(new BsonString("string")) === QString,
      qdataDecoder.tpe(new BsonBinary(Array[Byte]())) === QMeta,
      qdataDecoder.tpe(new BsonMinKey()) === QMeta,
      qdataDecoder.tpe(new BsonMaxKey()) === QMeta,
      qdataDecoder.tpe(new BsonBoolean(true)) === QBoolean,
      qdataDecoder.tpe(new BsonObjectId()) === QMeta,
      qdataDecoder.tpe(new BsonDateTime(1000)) === QOffsetDateTime,
      qdataDecoder.tpe(new BsonDocument()) === QObject,
      qdataDecoder.tpe(new BsonArray()) === QArray,
      qdataDecoder.tpe(new BsonDbPointer("db", new ObjectId())) === QMeta,
      qdataDecoder.tpe(new BsonTimestamp(112L)) === QMeta,
      qdataDecoder.tpe(new BsonUndefined()) === QNull,
      qdataDecoder.tpe(new BsonDecimal128(new Decimal128(112L))) === QReal,
      qdataDecoder.tpe(new BsonJavaScript("val a = undefined")) === QMeta,
      qdataDecoder.tpe(new BsonRegularExpression("*")) === QMeta,
      qdataDecoder.tpe(new BsonJavaScriptWithScope("val a = undefined", new BsonDocument())) === QMeta,
    ).forall(x => x)
  }
  "QLong values are correct" >> {
    List(
      qdataDecoder.getLong(new BsonInt64(12L)) === 12L,
      qdataDecoder.getLong(new BsonInt32(12)) === 12L
    ).forall(x => x)
  }

  "QDouble values are correct" >> {
    List(
      qdataDecoder.getDouble(new BsonDouble(12.2)) === 12.2,
      qdataDecoder.getDouble(new BsonDouble(-0.212)) === -0.212
    ).forall(x => x)
  }

  "QReal values are correct" >> {
    qdataDecoder.getReal(new BsonDecimal128(new Decimal128(112L))) === Real(112L),
  }

  "QBoolean values are correct" >> {
    List(
      qdataDecoder.getBoolean(new BsonBoolean(true)) === true,
      qdataDecoder.getBoolean(new BsonBoolean(false)) === false
    ).forall(x => x)
  }

  "QOffsetDateTime values are correct" >> {
    List(
      qdataDecoder.getOffsetDateTime(new BsonDateTime(123)) ===
        Instant.ofEpochMilli(123).atOffset(ZoneOffset.UTC),
    ).forall(x => x)
  }
  "QString values are correct" >> {
    List(
      qdataDecoder.getString(new BsonString("foo")) == "foo",
      qdataDecoder.getString(new BsonString("bar")) == "bar"
    ).forall(x => x)
  }
  "QArray works for empty arrays" >> {
    val bsonArr = new BsonArray()
    val cursor = qdataDecoder.getArrayCursor(bsonArr)
    qdataDecoder.hasNextArray(cursor) must beFalse
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
}
