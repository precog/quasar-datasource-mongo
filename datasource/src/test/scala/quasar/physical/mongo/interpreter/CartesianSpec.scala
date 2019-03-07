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
        (pipes must beTree(expected)) and (mapper === Mapper.Unfocus)
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
        "a" -> O.projection(Projection.key("root") + Projection.key("a")),
        "ba" -> O.projection(Projection.key("root") + Projection.key("b")),
        "bm" -> O.projection(Projection.key("root") + Projection.key("b")))),
      Pipeline.$project(Map(
        "a" -> O.string("$a"),
        "ba" -> O.$cond(
          O.$or(List(O.$eq(List(O.$type(O.key("ba")), O.string("array"))))),
          O.key("ba"),
          O.key("ba_non_existent_field")),
        "bm" -> O.string("$bm"))),
      Pipeline.$project(Map(
        "a" -> O.string("$a"),
        "ba" -> O.string("$ba"),
        "bm" -> O.string("$bm"),
        "ba_unwind" -> O.key("ba"))),
      Pipeline.$unwind("ba_unwind", "ba_unwind_index"),
      Pipeline.$project(Map(
        "a" -> O.string("$a"),
        "ba" -> O.string("$ba_unwind"),
        "bm" -> O.string("$bm"))),
      Pipeline.$project(Map(
        "a" -> O.string("$a"),
        "ba" -> O.string("$ba"),
        "bm" -> O.$cond(
          O.$or(List(O.$eq(List(O.$type(O.key("bm")), O.string("object"))))),
          O.key("bm"),
          O.key("bm_non_existent_field")))),
      Pipeline.$project(Map(
        "a" -> O.string("$a"),
        "ba" -> O.string("$ba"),
        "bm" -> O.string("$bm"),
        "bm_unwind" -> O.$objectToArray(O.key("bm")))),
      Pipeline.$unwind("bm_unwind", "bm_unwind_index"),
      Pipeline.$project(Map(
        "a" -> O.string("$a"),
        "ba" -> O.string("$ba"),
        "bm" -> O.string("$bm_unwind.v"))),
      Pipeline.$match(O.$or(List(
        O.obj(Map("a" -> O.$exists(O.bool(true)))),
        O.obj(Map("ba" -> O.$exists(O.bool(true)))),
        O.obj(Map("bm" -> O.$exists(O.bool(true))))))))


    evalCartesian(cartouches) must beLike {
      case Some((mapper, pipes)) =>
        (pipes must beTree(expected)) and (mapper === Mapper.Unfocus)
    }
  }
}
