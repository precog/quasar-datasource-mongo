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

package quasar.physical.mongo.interpreter

import slamdata.Predef._

import org.specs2.mutable.Specification

import quasar.api.table.ColumnType
import quasar.common.{CPath, CPathField}
import quasar.physical.mongo.{Interpreter, Version, PushdownLevel}
import quasar.{ScalarStage, IdStatus}
import quasar.physical.mongo.expression._
import quasar.physical.mongo.utils._

import scalaz.Scalaz._

class CartesianSpec extends Specification with quasar.TreeMatchers {
  def evalCartesian(
      cartouches: Map[CPathField, (CPathField, List[ScalarStage.Focused])])
      : Option[(Mapper, List[Pipe])] = {
    val interpreter = new Interpreter(Version.$objectToArray, "root", PushdownLevel.Full)
    optToAlternative[InState].apply(Projection.safeCartouches(cartouches))
      .flatMap((Cartesian[InState](_, interpreter.interpretStep[InState])))
      .run(InterpretationState("root", Mapper.Focus("root")))
      .map(_ leftMap (_.mapper))
  }
  "empty cartesian should erase everything" >> {
    val actual = evalCartesian(Map.empty)
    val expected = List(Pipeline.$match(O.obj(Map("root_cartesian_empty" -> O.bool(false)))))
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
      Pipeline.$project(Map(
        "_id" -> O.int(0),
        "roota" -> O.projection(Projection.key("root") + Projection.key("a")),
        "rootba" -> O.projection(Projection.key("root") + Projection.key("b")),
        "rootbm" -> O.projection(Projection.key("root") + Projection.key("b")))),
      Pipeline.$project(Map(
        "roota" -> O.string("$roota"),
        "rootba" -> O.$cond(
          O.$or(List(O.$eq(List(O.$type(O.key("rootba")), O.string("array"))))),
          O.key("rootba"),
          O.string("rootba_missing")),
        "rootbm" -> O.string("$rootbm"),
        "_id" -> O.int(0))),
      Pipeline.$project(Map(
        "roota" -> O.string("$roota"),
        "rootba" -> O.string("$rootba"),
        "rootbm" -> O.string("$rootbm"),
        "rootba_unwind" ->
          O.$cond(
            O.$or(List(
              O.$not(O.$eq(List(O.$type(O.key("rootba")), O.string("array")))),
              O.$eq(List(O.key("rootba"), O.array(List()))))),
            O.array(List(O.string("rootba_missing"))),
            O.key("rootba")))),
      Pipeline.$unwind("rootba_unwind", "rootba_unwind_index"),
      Pipeline.$project(Map(
        "roota" -> O.string("$roota"),
        "rootba" -> O.string("$rootba_unwind"),
        "rootbm" -> O.string("$rootbm"),
        "_id" -> O.int(0))),
      Pipeline.$project(Map(
        "roota" -> O.string("$roota"),
        "rootba" -> O.string("$rootba"),
        "rootbm" -> O.$cond(
          O.$or(List(O.$eq(List(O.$type(O.key("rootbm")), O.string("object"))))),
          O.key("rootbm"),
          O.string("rootbm_missing")),
        "_id" -> O.int(0))),
      Pipeline.$project(Map(
        "roota" -> O.string("$roota"),
        "rootba" -> O.string("$rootba"),
        "rootbm" -> O.string("$rootbm"),
        "rootbm_unwind" ->
          O.$cond(
            O.$or(List(
              O.$not(O.$eq(List(O.$type(O.key("rootbm")), O.string("object")))),
              O.$eq(List(O.key("rootbm"), O.obj(Map()))))),
            O.array(List(O.obj(Map(
              "k" -> O.string("rootbm_missing"),
              "v" -> O.string("rootbm_missing"))))),
            O.$objectToArray(O.key("rootbm"))))),
      Pipeline.$unwind("rootbm_unwind", "rootbm_unwind_index"),
      Pipeline.$project(Map(
        "roota" -> O.string("$roota"),
        "rootba" -> O.string("$rootba"),
        "rootbm" -> O.string("$rootbm_unwind.v"),
        "_id" -> O.int(0))),
      Pipeline.$project(Map(
        "a" -> O.$cond(
          O.$eq(List(O.string("$roota"), O.string("roota_missing"))),
          O.string("$roota_missing"),
          O.string("$roota")),
        "ba" -> O.$cond(
          O.$eq(List(O.string("$rootba"), O.string("rootba_missing"))),
          O.string("$rootba_missing"),
          O.string("$rootba")),
        "bm" -> O.$cond(
          O.$eq(List(O.string("$rootbm"), O.string("rootbm_missing"))),
          O.string("$rootbm_missing"),
          O.string("$rootbm")),
      )),
      Pipeline.$match(O.$or(List(
        O.obj(Map("a" -> O.$exists(O.bool(true)))),
        O.obj(Map("ba" -> O.$exists(O.bool(true)))),
        O.obj(Map("bm" -> O.$exists(O.bool(true))))))))


    evalCartesian(cartouches) must beLike {
      case Some((mapper, pipes)) =>
        (pipes must beTree(expected)) and (mapper must_=== Mapper.Unfocus)
    }
  }
}
