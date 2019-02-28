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

import quasar.api.table.ColumnType
import quasar.physical.mongo.expression._
import quasar.IdStatus

object Pivot {
  def apply(uniqueKey: String, status: IdStatus, vectorType: ColumnType.Vector): Option[List[Pipe]] = {
    val unwindKey = uniqueKey.concat("_unwind")
    val indexKey = uniqueKey.concat("_unwind_index")
    val unwind = Pipeline.$unwind(unwindKey, indexKey)

    // input to a `Pivot` is guaranteed to be `Mask`ed with the appropriate type. i.e.
    // there will always be, effectively, `Mask(. -> Object), Pivot(_, Object)`.
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
