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

import com.mongodb.ConnectionString

import quasar.physical.mongo.MongoResource.{Database, Collection}

final case class MongoConfig(
    connectionString: String,
    resultBatchSizeBytes: Option[Int],
    disablePushdown: Option[Boolean]) {
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
      "disablePushdown")

  private val credentialsRegex = "://[^@+]+@".r

  def sanitize(config: Json): Json = config.as[MongoConfig].result match {
    case Left(_) => config
    case Right(cfg) => {
      val newConnectionString = credentialsRegex.replaceFirstIn(cfg.connectionString, "://<REDACTED>:<REDACTED>@")
      cfg.copy(connectionString = newConnectionString).asJson
    }
  }
}
