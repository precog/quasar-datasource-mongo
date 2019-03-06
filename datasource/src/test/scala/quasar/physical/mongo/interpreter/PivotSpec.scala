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

import org.specs2.mutable.Specification

import quasar.api.table.ColumnType
import quasar.physical.mongo.expression._
import quasar.IdStatus

import scalaz.{State, Scalaz}, Scalaz._

class PivotSpec extends Specification with quasar.TreeMatchers {
  private def evalPivot(
      state: InterpretationState,
      idStatus: IdStatus,
      columnType: ColumnType.Vector)
      : (Mapper, List[Pipe]) =
    Pivot[State[InterpretationState, ?]](idStatus, columnType) run state leftMap (_.mapper)


  "Array examples" >> {
    val initialState = InterpretationState("root", Mapper.Unfocus)
    def mkExpected(e: Expr): List[Pipe] = List(
      Pipeline.$project(Map("root_unwind" ->
        O.$cond(
          O.$eq(List(O.$type(O.steps(List())), O.string("array"))),
          O.steps(List()),
          O.array(List(O.string("root_pivot_undefined")))))),
      Pipeline.$unwind("root_unwind", "root_unwind_index"),
      Pipeline.$project(Map("root" -> e)),
      Pipeline.PivotFilter("root"))

    "id only" >> {
      val actual = evalPivot(initialState, IdStatus.IdOnly, ColumnType.Array)
      val expected = mkExpected(O.string("$root_unwind_index"))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
    "values only" >> {
      val actual = evalPivot(initialState, IdStatus.ExcludeId, ColumnType.Array)
      val expected = mkExpected(O.string("$root_unwind"))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
    "both" >> {
      val actual = evalPivot(initialState, IdStatus.IncludeId, ColumnType.Array)
      val expected = mkExpected(O.array(List(O.string("$root_unwind_index"), O.string("$root_unwind"))))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
  }

  "Object examples" >> {
    val initialState = InterpretationState("root", Mapper.Unfocus)

    def mkExpected(e: Expr): List[Pipe] = List(
      Pipeline.$project(Map("root_unwind" ->
        O.$cond(
          O.$eq(List(O.$type(O.steps(List())), O.string("object"))),
          O.$objectToArray(O.steps(List())),
          O.array(List(O.obj(Map(
            "k" -> O.string("root_pivot_undefined"),
            "v" -> O.string("root_pivot_undefined")))))))),
      Pipeline.$unwind("root_unwind", "root_unwind_index"),
      Pipeline.$project(Map("root" -> e)),
      Pipeline.PivotFilter("root"))

    "id only" >> {
      val actual = evalPivot(initialState, IdStatus.IdOnly, ColumnType.Object)
      val expected = mkExpected(O.string("$root_unwind.k"))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
    "values only" >> {
      val actual = evalPivot(initialState, IdStatus.ExcludeId, ColumnType.Object)
      val expected = mkExpected(O.string("$root_unwind.v"))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
    "both" >> {
      val actual = evalPivot(initialState, IdStatus.IncludeId, ColumnType.Object)
      val expected = mkExpected(O.array(List(
        O.string("$root_unwind.k"),
        O.string("$root_unwind.v"))))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
  }

  "Pivot in contexts" >> {
    val initialState = InterpretationState("unique", Mapper.Focus("focused"))
    "array" >> {
      val actual = evalPivot(initialState, IdStatus.ExcludeId, ColumnType.Array)
      val expected = List(
        Pipeline.$project(Map("unique_unwind" ->
          O.$cond(
            O.$eq(List(O.$type(O.key("focused")), O.string("array"))),
            O.key("focused"),
            O.array(List(O.string("unique_pivot_undefined")))))),
        Pipeline.$unwind("unique_unwind", "unique_unwind_index"),
        Pipeline.$project(Map("unique" -> O.string("$unique_unwind"))),
        Pipeline.PivotFilter("unique")
      )
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("unique"))
    }
    "object" >> {
      val actual = evalPivot(initialState, IdStatus.ExcludeId, ColumnType.Object)
      val expected = List(
        Pipeline.$project(Map("unique_unwind" ->
          O.$cond(
            O.$eq(List(O.$type(O.key("focused")), O.string("object"))),
            O.$objectToArray(O.key("focused")),
            O.array(List(O.obj(Map(
              "k" -> O.string("unique_pivot_undefined"),
              "v" -> O.string("unique_pivot_undefined")))))))),
        Pipeline.$unwind("unique_unwind", "unique_unwind_index"),
        Pipeline.$project(Map("unique" -> O.string("$unique_unwind.v"))),
        Pipeline.PivotFilter("unique")
      )
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("unique"))
    }
  }
}
