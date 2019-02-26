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
import quasar.physical.mongo.{MongoInterpreter, Version, PushdownLevel}
import quasar.{ScalarStage, IdStatus}
import quasar.physical.mongo.expression._

class CartesianSpec extends Specification {
  def evalCartesian(cartouches: Map[CPathField, (CPathField, List[ScalarStage.Focused])]): Option[List[Pipe]] = {
    val interpreter = new MongoInterpreter(Version.$objectToArray, "root", PushdownLevel.Full)
    Projection.safeCartouches(cartouches) flatMap { x => Cartesian("root", x, interpreter) }

  }
  "empty cartesian should erase everything" >> {
    val actual = evalCartesian(Map.empty)
    val expected = Some(List(Pipeline.$match(Map("root_cartesian_empty" -> O.bool(false)))))
    actual === expected
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

    val actual = evalCartesian(cartouches)

    val expected = Some(List(
      Pipeline.$project(Map(
        "a" -> O.projection(Projection.key("root") + Projection.key("a")),
        "ba" -> O.projection(Projection.key("root") + Projection.key("b")),
        "bm" -> O.projection(Projection.key("root") + Projection.key("b")))),
      Pipeline.$project(Map(
        "a" -> O.key("a"),
        "ba" -> O.$cond(
          O.$or(List(O.$eq(List(O.$type(O.key("ba")), O.string("array"))))),
          O.key("ba"),
          O.key("ba_non_existent_field")),
        "bm" -> O.key("bm"))),
      Pipeline.$project(Map(
        "a" -> O.key("a"),
        "ba" -> O.key("ba"),
        "bm" -> O.key("bm"),
        "ba_unwind" -> O.key("ba"))),
      Pipeline.$unwind("ba_unwind", "ba_unwind_index"),
      Pipeline.$project(Map(
        "a" -> O.key("a"),
        "ba" -> O.key("ba_unwind"),
        "bm" -> O.key("bm"))),
      Pipeline.$project(Map(
        "a" -> O.key("a"),
        "ba" -> O.key("ba"),
        "bm" -> O.$cond(
          O.$or(List(O.$eq(List(O.$type(O.key("bm")), O.string("object"))))),
          O.key("bm"),
          O.key("bm_non_existent_field")))),
      Pipeline.$project(Map(
        "a" -> O.key("a"),
        "ba" -> O.key("ba"),
        "bm" -> O.key("bm"),
        "bm_unwind" -> O.$objectToArray(O.key("bm")))),
      Pipeline.$unwind("bm_unwind", "bm_unwind_index"),
      Pipeline.$project(Map(
        "a" -> O.key("a"),
        "ba" -> O.key("ba"),
        "bm" -> O.projection(Projection.key("bm_unwind") + Projection.key("v")))),
      Pipeline.$project(Map(
        "root" -> O.obj(Map(
          "a" -> O.key("a"),
          "ba" -> O.key("ba"),
          "bm" -> O.key("bm"))))),
      Pipeline.NotNull("root")))

    actual === expected
  }
}
