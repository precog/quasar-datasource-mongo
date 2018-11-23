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

final case class MongoConfig(connectionString: String)

object MongoConfig {
  implicit val encodeJson: EncodeJson[MongoConfig] =
    EncodeJson(conn => Json.obj("connectionString" -> jString(conn.connectionString)))

  implicit val decodeJson: DecodeJson[MongoConfig] = DecodeJson(config =>
    config.get[String]("connectionString").toOption match {
      case None => DecodeResult.fail("Incorrect mongodb configuration", config.history)
      case Some(unchecked) => isConnectionString(unchecked) match {
        case Right(str) => DecodeResult.ok(MongoConfig(str))
        case Left(msg) => DecodeResult.fail(msg, config.history)
      }
    })

  // I don't think that this is _really_ necessary check, because `Mongo.apply(config)`
  // calls `new MongoClient` that calls `new ConnectionString` in its internals
  private def isConnectionString(inp: String): Either[String, String] = {
    try {
      val _ = new ConnectionString(inp)
      Right(inp)
    }
    catch {
      case e: Throwable => Left(e.getMessage())
    }
  }

  private val credentialsRegex = "://[^@+]+@".r

  def sanitize(config: Json): Json = config.as[MongoConfig].result match {
    case Left(_) => config
    case Right(MongoConfig(value)) => {
      MongoConfig(credentialsRegex.replaceFirstIn(value, "://<REDACTED>:<REDACTED>@")).asJson
    }
  }
}
