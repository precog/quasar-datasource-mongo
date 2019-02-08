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

package quasar.physical.mongo.interpreter

import slamdata.Predef._

import cats.syntax.order._


import quasar.api.table.ColumnType
import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}
import quasar.IdStatus

import shims._

object Pivot {
  val IndexKey: String = "index"

  def columnTypeVectorString(p: ColumnType.Vector): String = p match {
    case ColumnType.Array => "array"
    case ColumnType.Object => "object"
  }

  def matchStructure(path: E.Projection, p: ColumnType.Vector): Aggregator =
    Aggregator.filter(E.Object("typeTag" -> E.String(columnTypeVectorString(p))))

  def toArray(projection: E.Projection, p: ColumnType.Vector): List[Aggregator] = p match {
    case ColumnType.Array => List()
    case ColumnType.Object => List(Aggregator.project(E.Object(projection.toKey -> E.Object("$objectToArray" -> projection))))
  }

  def mkValue(status: IdStatus, p: ColumnType.Vector, unwindedKey: E.Projection): MongoExpression = p match {
    case ColumnType.Array => status match {
      case IdStatus.IdOnly => E.key(IndexKey)
      case IdStatus.ExcludeId => unwindedKey
      case IdStatus.IncludeId => E.Array(E.key(IndexKey), unwindedKey)
    }
    case ColumnType.Object => status match {
      case IdStatus.IdOnly => unwindedKey +/ E.key("k")
      case IdStatus.ExcludeId => unwindedKey +/ E.key("v")
      case IdStatus.IncludeId => E.Array(unwindedKey +/ E.key("k"), unwindedKey +/ E.key("v"))
    }
  }

  def apply(
      uniqueKey: String,
      version: Version,
      status: IdStatus,
      vectorType: ColumnType.Vector)
      : Option[List[Aggregator]] = {

    if (version < Version(3, 4, 0)) None
    else Some {
      val projection = E.key(uniqueKey)
      val addTypeTag = Aggregator.project(E.Object(
        uniqueKey -> E.key(uniqueKey),
        "typeTag" -> E.Object("$type" -> projection)))
      val filter = matchStructure(projection, vectorType)
      val mkArray = toArray(projection, vectorType)
      val unwind = Aggregator.unwind(projection, IndexKey)
      val toSet = mkValue(status, vectorType, projection)
      val setProjection = Aggregator.project(E.Object(projection.toKey -> toSet))
      List(addTypeTag, filter) ++ mkArray ++ List(unwind, setProjection)
    }
  }
}
