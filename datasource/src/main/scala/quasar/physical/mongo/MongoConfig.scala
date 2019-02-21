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

import argonaut._, Argonaut._

import cats.kernel.Order

import com.mongodb.ConnectionString

import quasar.physical.mongo.MongoResource.{Database, Collection}

trait PushdownLevel extends Product with Serializable
final case object Disabled extends PushdownLevel
final case object Light extends PushdownLevel
final case object Full extends PushdownLevel

object PushdownLevel {
  implicit val orderPushdownLevel: Order[PushdownLevel] = new Order[PushdownLevel] {
    def compare(a: PushdownLevel, b: PushdownLevel) = a match {
      case Disabled => b match {
        case Disabled => 0
        case _ => -1
      }
      case Light => b match {
        case Disabled => 1
        case Light => 0
        case Full => -1
      }
      case Full => b match {
        case Full => 0
        case _ => 1
      }
    }
  }

  implicit val encodePushdownLevel: EncodeJson[PushdownLevel] = new EncodeJson[PushdownLevel] {
    def encode(p: PushdownLevel): Json = p match {
      case Disabled => jString("disabled")
      case Light => jString("light")
      case Full => jString("full")
    }
  }
  implicit val decodePushdownLevel: DecodeJson[PushdownLevel] = new DecodeJson[PushdownLevel] {
    def decode(j: HCursor): DecodeResult[PushdownLevel] = j.as[String] flatMap {
      case "disabled" => DecodeResult.ok(Disabled)
      case "light" => DecodeResult.ok(Light)
      case "full" => DecodeResult.ok(Full)
      case _ => DecodeResult.fail("This is not PushdownLevel", CursorHistory(List()))
    }
  }

}

final case class MongoConfig(
    connectionString: String,
    resultBatchSizeBytes: Option[Int],
    pushdownLevel: Option[PushdownLevel]) {
  def accessedResource: Option[MongoResource] = {
    val connString = new ConnectionString(connectionString)
    val dbStr = Option(connString.getDatabase())
    val collStr = Option(connString.getCollection())
    dbStr.map(Database(_)).flatMap((db: Database) => collStr match {
      case None => Some(db)
      case Some(cn) => Some(Collection(db, cn))
    })
  }
}

object MongoConfig {

  implicit val codecMongoConfig: CodecJson[MongoConfig] =
    casecodec3(MongoConfig.apply, MongoConfig.unapply)(
      "connectionString",
      "resultBatchSizeBytes",
      "pushdownLevel")

  private val credentialsRegex = "://[^@+]+@".r

  def sanitize(config: Json): Json = config.as[MongoConfig].result match {
    case Left(_) => config
    case Right(cfg) => {
      val newConnectionString = credentialsRegex.replaceFirstIn(cfg.connectionString, "://<REDACTED>:<REDACTED>@")
      cfg.copy(connectionString = newConnectionString).asJson
    }
  }
}
