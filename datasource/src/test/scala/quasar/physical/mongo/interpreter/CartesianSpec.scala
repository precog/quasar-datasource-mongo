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

import quasar.{ScalarStage, IdStatus}
import quasar.api.ColumnType
import quasar.RenderTree._
import quasar.common.{CPath, CPathField}
import quasar.contrib.iotac._
import quasar.physical.mongo.{Interpreter, PushdownLevel}
import quasar.physical.mongo.expression._

import cats.implicits._
import cats.mtl.implicits._
import higherkindness.droste.data.Fix

class CartesianSpec extends Specification with quasar.TreeMatchers {
  val o = Optics.full(Optics.basisPrism[Expr, Fix[Expr]])
  def evalCartesian(
      cartouches: Map[CPathField, (CPathField, List[ScalarStage.Focused])])
      : Option[(Mapper, List[Pipeline[Fix[Expr]]])] = {
    val cartesian = Cartesian[InState, Fix[Expr]](Interpreter.interpretStage[InState, Fix[Expr]](PushdownLevel.Full))
    optToAlternative[InState].apply(Projection.safeCartouches(cartouches))
      .flatMap(x => cartesian(ScalarStage.Cartesian(cartouches)))
      .run(InterpretationState("root", Mapper.Focus("root")))
      .map(_ leftMap (_.mapper))
  }

  "empty cartesian should erase everything" >> {
    val actual = evalCartesian(Map.empty)
    val expected = List(Pipeline.Match(o.obj(Map("root_cartesian_empty" -> o.bool(false)))): Pipeline[Fix[Expr]])
    actual must beLike {
      case Some((mapper, pipes)) =>
        (pipes must beTree(expected)) and (mapper must_=== Mapper.Unfocus)
    }
  }

  "example" >> {

    val cartouches = Map(
      (CPathField("a"), (CPathField("a"), Nil)),

      (CPathField("ba"), (CPathField("b"), List(
        ScalarStage.Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
        ScalarStage.Pivot(IdStatus.ExcludeId, ColumnType.Array)))),

      (CPathField("bm"), (CPathField("b"), List(
        ScalarStage.Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
        ScalarStage.Pivot(IdStatus.ExcludeId, ColumnType.Object)))))

    val expected = List(
      Pipeline.Project(Map(
        "_id" -> o.int(0),
        "roota" -> o.projection(Projection.key("root") + Projection.key("a")),
        "rootba" -> o.projection(Projection.key("root") + Projection.key("b")),
        "rootbm" -> o.projection(Projection.key("root") + Projection.key("b")))),
      Pipeline.Project(Map(
        "roota" -> o.str("$roota"),
        "rootba" -> o.cond(
          o.or(List(o.eqx(List(o.typ(o.key("rootba")), o.str("array"))))),
          o.key("rootba"),
          o.str("rootba_missing")),
        "rootbm" -> o.str("$rootbm"),
        "_id" -> o.int(0))),
      Pipeline.Project(Map(
        "roota" -> o.str("$roota"),
        "rootba" -> o.str("$rootba"),
        "rootbm" -> o.str("$rootbm"),
        "rootba_unwind" ->
          o.cond(
            o.or(List(
              o.not(o.eqx(List(o.typ(o.key("rootba")), o.str("array")))),
              o.eqx(List(o.key("rootba"), o.array(List()))))),
            o.array(List(o.str("rootba_missing"))),
            o.key("rootba")))),
      Pipeline.Unwind("rootba_unwind", "rootba_unwind_index"),
      Pipeline.Project(Map(
        "roota" -> o.str("$roota"),
        "rootba" -> o.str("$rootba_unwind"),
        "rootbm" -> o.str("$rootbm"),
        "_id" -> o.int(0))),
      Pipeline.Project(Map(
        "roota" -> o.str("$roota"),
        "rootba" -> o.str("$rootba"),
        "rootbm" -> o.cond(
          o.or(List(o.eqx(List(o.typ(o.key("rootbm")), o.str("object"))))),
          o.key("rootbm"),
          o.str("rootbm_missing")),
        "_id" -> o.int(0))),
      Pipeline.Project(Map(
        "roota" -> o.str("$roota"),
        "rootba" -> o.str("$rootba"),
        "rootbm" -> o.str("$rootbm"),
        "rootbm_unwind" ->
          o.cond(
            o.or(List(
              o.not(o.eqx(List(o.typ(o.key("rootbm")), o.str("object")))),
              o.eqx(List(o.key("rootbm"), o.obj(Map()))))),
            o.array(List(o.obj(Map(
              "k" -> o.str("rootbm_missing"),
              "v" -> o.str("rootbm_missing"))))),
            o.objectToArray(o.key("rootbm"))))),
      Pipeline.Unwind("rootbm_unwind", "rootbm_unwind_index"),
      Pipeline.Project(Map(
        "roota" -> o.str("$roota"),
        "rootba" -> o.str("$rootba"),
        "rootbm" -> o.str("$rootbm_unwind.v"),
        "_id" -> o.int(0))),
      Pipeline.Project(Map(
        "a" -> o.cond(
          o.eqx(List(o.str("$roota"), o.str("roota_missing"))),
          o.str("$roota_missing"),
          o.str("$roota")),
        "ba" -> o.cond(
          o.eqx(List(o.str("$rootba"), o.str("rootba_missing"))),
          o.str("$rootba_missing"),
          o.str("$rootba")),
        "bm" -> o.cond(
          o.eqx(List(o.str("$rootbm"), o.str("rootbm_missing"))),
          o.str("$rootbm_missing"),
          o.str("$rootbm")),
      )),
      Pipeline.Match(o.or(List(
        o.obj(Map("a" -> o.exists(o.bool(true)))),
        o.obj(Map("ba" -> o.exists(o.bool(true)))),
        o.obj(Map("bm" -> o.exists(o.bool(true))))))))


    evalCartesian(cartouches) must beLike {
      case Some((mapper, pipes)) =>
        (pipes must beTree(expected)) and (mapper must_=== Mapper.Unfocus)
    }
  }
}
