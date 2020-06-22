/*
 * Copyright 2020 Precog Data
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

import quasar.{IdStatus, ScalarStage}
import quasar.api.ColumnType
import quasar.contrib.iotac._
import quasar.physical.mongo.expression._

import cats.implicits._
import cats.mtl.implicits._
import higherkindness.droste.data.Fix


class PivotSpec extends Specification with quasar.TreeMatchers {
  private def evalPivot(
      state: InterpretationState,
      idStatus: IdStatus,
      columnType: ColumnType.Vector)
      : (Mapper, List[Pipeline[Fix[Expr]]]) = {
    val interpreter = Pivot[InState, Fix[Expr]]
    interpreter(ScalarStage.Pivot(idStatus, columnType)).run(state).get.leftMap(_.mapper)
  }

  val o = Optics.full(Optics.basisPrism[Expr, Fix[Expr]])

  "Array examples" >> {
    val initialState = InterpretationState("root", Mapper.Unfocus)
    def mkExpected(e: Fix[Expr]): List[Pipeline[Fix[Expr]]] = List(
      Pipeline.Project(Map("root_unwind" ->
        o.cond(
          o.or(List(
            o.not(o.eqx(List(o.typ(o.steps(List())), o.str("array")))),
            o.eqx(List(o.steps(List()), o.array(List()))))),
          o.array(List(o.str("root_missing"))),
          o.steps(List())))),
      Pipeline.Unwind("root_unwind", "root_unwind_index"),
      Pipeline.Project(Map("_id" -> o.int(0), "root" -> e)),
      Pipeline.Presented)

    "id only" >> {
      val actual = evalPivot(initialState, IdStatus.IdOnly, ColumnType.Array)
      val expected = mkExpected(
        o.cond(
          o.eqx(List(o.str("$root_unwind"), o.str("root_missing"))),
          o.str("root_missing"),
          o.str("$root_unwind_index")))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
    "values only" >> {
      val actual = evalPivot(initialState, IdStatus.ExcludeId, ColumnType.Array)
      val expected = mkExpected(o.str("$root_unwind"))

      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
    "both" >> {
      val actual = evalPivot(initialState, IdStatus.IncludeId, ColumnType.Array)
      val expected = mkExpected(
        o.cond(
          o.eqx(List(o.str("$root_unwind"), o.str("root_missing"))),
          o.str("root_missing"),
          o.array(List(o.str("$root_unwind_index"), o.str("$root_unwind")))))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
  }

  "Object examples" >> {
    val initialState = InterpretationState("root", Mapper.Unfocus)

    def mkExpected(e: Fix[Expr]): List[Pipeline[Fix[Expr]]] = List(
      Pipeline.Project(Map("root_unwind" ->
        o.cond(
          o.or(List(
            o.not(o.eqx(List(o.typ(o.steps(List())), o.str("object")))),
            o.eqx(List(o.steps(List()), o.obj(Map()))))),
          o.array(List(o.obj(Map(
            "k" -> o.str("root_missing"),
            "v" -> o.str("root_missing"))))),
          o.objectToArray(o.steps(List()))))),
      Pipeline.Unwind("root_unwind", "root_unwind_index"),
      Pipeline.Project(Map("_id" -> o.int(0), "root" -> e)),
      Pipeline.Presented)

    "id only" >> {
      val actual = evalPivot(initialState, IdStatus.IdOnly, ColumnType.Object)
      val expected = mkExpected(o.str("$root_unwind.k"))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
    "values only" >> {
      val actual = evalPivot(initialState, IdStatus.ExcludeId, ColumnType.Object)
      val expected = mkExpected(o.str("$root_unwind.v"))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
    "both" >> {
      val actual = evalPivot(initialState, IdStatus.IncludeId, ColumnType.Object)
      val expected = mkExpected(o.cond(
        o.eqx(List(o.str("$root_unwind.v"), o.str("root_missing"))),
        o.str("root_missing"),
        o.array(List(
          o.str("$root_unwind.k"),
          o.str("$root_unwind.v")))))
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("root"))
    }
  }

  "Pivot in contexts" >> {
    val initialState = InterpretationState("unique", Mapper.Focus("focused"))
    "array" >> {
      val actual = evalPivot(initialState, IdStatus.ExcludeId, ColumnType.Array)
      val expected = List(
        Pipeline.Project(Map("unique_unwind" ->
          o.cond(
            o.or(List(
              o.not(o.eqx(List(o.typ(o.key("focused")), o.str("array")))),
              o.eqx(List(o.key("focused"), o.array(List()))))),
          o.array(List(o.str("unique_missing"))),
          o.key("focused")))),
        Pipeline.Unwind("unique_unwind", "unique_unwind_index"),
        Pipeline.Project(Map("_id" -> o.int(0), "unique" -> o.str("$unique_unwind"))),
        Pipeline.Presented)
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("unique"))
    }
    "object" >> {
      val actual = evalPivot(initialState, IdStatus.ExcludeId, ColumnType.Object)
      val expected = List(
        Pipeline.Project(Map("unique_unwind" ->
          o.cond(
            o.or(List(
              o.not(o.eqx(List(o.typ(o.key("focused")), o.str("object")))),
              o.eqx(List(o.key("focused"), o.obj(Map()))))),
            o.array(List(o.obj(Map(
              "k" -> o.str("unique_missing"),
              "v" -> o.str("unique_missing"))))),
            o.objectToArray(o.key("focused"))))),

        Pipeline.Unwind("unique_unwind", "unique_unwind_index"),
        Pipeline.Project(Map("_id" -> o.int(0), "unique" -> o.str("$unique_unwind.v"))),
        Pipeline.Presented)
      (actual._2 must beTree(expected)) and (actual._1 === Mapper.Focus("unique"))
    }
  }
}
