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

import scalaz.{MonadState, Scalaz}, Scalaz._

object Pivot {
  def ensureArray(vectorType: ColumnType.Vector, key: String, undefined: Expr): Pipe = {
    val proj = O.steps(List())
    Pipeline.$project(Map(key -> (vectorType match {
      case ColumnType.Object =>
        O.$cond(
          O.$or(List(O.$not(O.$eq(List(O.$type(proj), O.string("object")))), O.$eq(List(proj, O.obj(Map()))))),
          O.array(List(O.obj(Map("k" -> undefined, "v" -> undefined)))),
          O.$objectToArray(O.steps(List())))

      case ColumnType.Array =>
        O.$cond(
          O.$or(List(O.$not(O.$eq(List(O.$type(proj), O.string("array")))), O.$eq(List(proj, O.array(List()))))),
          O.array(List(undefined)),
          proj)

    })))
  }

  def mkValue(
      status: IdStatus,
      vectorType: ColumnType.Vector,
      unwinded: String,
      index: String,
      undefined: Expr)
      : Expr = {
    val indexString = O.string("$" concat index)
    val unwindString = O.string("$" concat unwinded)
    val kString = O.string("$" concat unwinded concat ".k")
    val vString = O.string("$" concat unwinded concat ".v")
    vectorType match {
      case ColumnType.Array => status match {
        case IdStatus.IdOnly =>
          O.$cond(
            O.$eq(List(unwindString, undefined)),
            undefined,
            indexString)
        case IdStatus.ExcludeId =>
          unwindString
        case IdStatus.IncludeId => O.array(List(
          O.$cond(
            O.$eq(List(unwindString, undefined)),
            undefined,
            indexString),
          unwindString))
      }
      case ColumnType.Object => status match {
        case IdStatus.IdOnly => kString
        case IdStatus.ExcludeId => vString
        case IdStatus.IncludeId => O.array(List(kString, vString))
      }
    }
  }

  def mkPipes(
      status: IdStatus,
      vectorType: ColumnType.Vector,
      state: InterpretationState)
      : List[Pipe] = {
    val unwindKey = state.uniqueKey concat "_unwind"
    val indexKey = state.uniqueKey concat "_unwind_index"

    List(
      ensureArray(vectorType, unwindKey, pivotUndefined(state.uniqueKey)),
      Pipeline.$unwind(unwindKey, indexKey),
      Pipeline.$project(Map(
        "_id" -> O.int(0),
        state.uniqueKey -> mkValue(status, vectorType, unwindKey, indexKey, pivotUndefined(state.uniqueKey)))),
      Pipeline.PivotFilter(state.uniqueKey))
  }


  def apply[F[_]: MonadInState](status: IdStatus, vectorType: ColumnType.Vector): F[List[Pipe]] =
    for {
      state <- MonadState[F, InterpretationState].get
      res = mkPipes(status, vectorType, state)
      _ <- focus[F]
    } yield res map mapProjection(state.mapper)
}
