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

import argonaut._, Argonaut._

import com.mongodb.ConnectionString

import quasar.physical.mongo.MongoResource.{Database, Collection}

final case class MongoConfig(
    connectionString: String,
    batchSize: Int,
    pushdownLevel: PushdownLevel,
    tunnelConfig: Option[TunnelConfig],
    sslConfig: Option[SSLConfig] = None)
    extends Product with Serializable {
  def accessedResource: Option[MongoResource] = {
    val connString = new ConnectionString(connectionString)
    val dbStr = Option(connString.getDatabase())
    val collStr = Option(connString.getCollection())
    dbStr map(Database(_)) flatMap { (db: Database) => collStr match {
      case None => Some(db)
      case Some(cn) => Some(Collection(db, cn))
    }}
  }
}

object MongoConfig {
  val Migration1to2BatchSize: Int = 64
  val Migration1to2PushdownLevel: PushdownLevel = PushdownLevel.Disabled

  implicit val encodeJsonMongoConfig: EncodeJson[MongoConfig] = EncodeJson { (config: MongoConfig) =>
    Json(
      "connectionString" := config.connectionString,
      "batchSize" := config.batchSize,
      "pushdownLevel" := config.pushdownLevel,
      "tunnelConfig" := config.tunnelConfig,
      "sslConfig" := config.sslConfig)
  }

  implicit val decodeJsonMongoConfig: DecodeJson[MongoConfig] = DecodeJson { c =>
    for {
      connectionString <- (c --\ "connectionString").as[String]
      batchSize <- (c --\ "batchSize").as[Option[Int]]
      pushdownLevel <- (c --\ "pushdownLevel").as[Option[PushdownLevel]]
      tunnelConfig <- (c --\ "tunnelConfig").as[Option[TunnelConfig]]
      sslConfig <- (c --\ "sslConfig").as[Option[SSLConfig]]
    } yield MongoConfig(
      connectionString,
      batchSize getOrElse Migration1to2BatchSize,
      pushdownLevel getOrElse Migration1to2PushdownLevel,
      tunnelConfig,
      sslConfig)
  }

  private val credentialsRegex = "://[^@+]+@".r

  def sanitize(config: Json): Json = config.as[MongoConfig].result match {
    case Left(_) => config
    case Right(cfg) => {
      val newConnectionString = credentialsRegex.replaceFirstIn(cfg.connectionString, "://<REDACTED>:<REDACTED>@")
      val newTunnelConfig = cfg.tunnelConfig map TunnelConfig.sanitize
      val newSSLConfig = cfg.sslConfig map SSLConfig.sanitize
      cfg.copy(connectionString = newConnectionString, tunnelConfig = newTunnelConfig, sslConfig = newSSLConfig).asJson
    }
  }
}
