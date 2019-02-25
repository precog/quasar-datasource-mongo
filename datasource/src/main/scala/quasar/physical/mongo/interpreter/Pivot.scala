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
import quasar.physical.mongo.expression._
import quasar.IdStatus

import shims._

object Pivot {
  def columnTypeVectorString(p: ColumnType.Vector): String = p match {
    case ColumnType.Array => "array"
    case ColumnType.Object => "object"
  }

  // input to a `Pivot` is guaranteed to be `Mask`ed with the appropriate type. i.e.
  // there will always be, effectively, `Mask(. -> Object), Pivot(_, Object)`.
  def toArray(key: String, projection: E.Projection, p: ColumnType.Vector): Aggregator = {
    val refined: MongoExpression = p match {
      case ColumnType.Array => projection
      case ColumnType.Object => E.Object("$objectToArray" -> projection)
    }
    Aggregator.project(E.Object(key -> refined))
  }

  def mkValue(status: IdStatus, p: ColumnType.Vector, unwindedKey: E.Projection, indexKey: String): MongoExpression = p match {
    case ColumnType.Array => status match {
      case IdStatus.IdOnly => E.key(indexKey)
      case IdStatus.ExcludeId => unwindedKey
      case IdStatus.IncludeId => E.Array(E.key(indexKey), unwindedKey)
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

    if ((version < Version.$objectToArray && vectorType === ColumnType.Object) || version < Version.$unwind) None
    else Some {
      val projection =
        E.key(uniqueKey)

      val unwindKey =
        uniqueKey.concat("_unwind")

      val indexKey =
        uniqueKey.concat("_unwind_index")

      val unwind =
        Aggregator.unwind(E.key(unwindKey), indexKey)

      val toSet =
        mkValue(status, vectorType, E.key(unwindKey), indexKey)

      val setProjection =
        Aggregator.project(E.Object(projection.toKey -> toSet))

      List(toArray(unwindKey, projection, vectorType), unwind, setProjection, Aggregator.notNull(uniqueKey))
    }
  }
  def apply0(uniqueKey: String, status: IdStatus, vectorType: ColumnType.Vector): Option[List[Pipe]] = {
    val unwindKey = uniqueKey.concat("_unwind")
    val indexKey = uniqueKey.concat("_unwind_index")
    val unwind = Pipeline.$unwind(unwindKey, indexKey)

    val valueToSet = vectorType match {
      case ColumnType.Array => status match {
        case IdStatus.IdOnly => O.key(indexKey)
        case IdStatus.ExcludeId => O.key(unwindKey)
        case IdStatus.IncludeId => O.array(List(O.key(indexKey), O.key(unwindKey)))
      }
      case ColumnType.Object => status match {
        case IdStatus.IdOnly => O.projection(Projection.key(unwindKey) + Projection.key("k"))
        case IdStatus.ExcludeId => O.projection(Projection.key(unwindKey) + Projection.key("v"))
        case IdStatus.IncludeId =>
          O.array(List(
            O.projection(Projection.key(unwindKey) + Projection.key("k")),
            O.projection(Projection.key(unwindKey) + Projection.key("v"))))
      }
    }

    val setProjection = Pipeline.$project(Map(uniqueKey -> valueToSet))

    val toArray =
      Pipeline.$project(Map(unwindKey -> (vectorType match {
        case ColumnType.Object => O.$objectToArray(O.key(uniqueKey))
        case ColumnType.Array => O.key(uniqueKey)
      })))

    Some(List(toArray, unwind, setProjection, Pipeline.NotNull(uniqueKey)))
  }
}
