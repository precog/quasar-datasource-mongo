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
import quasar.physical.mongo.{MongoInterpreter, Version, Aggregator, MongoExpression => E}
import quasar.{ScalarStage, IdStatus}

class CartesianSpec extends Specification {
  import E.helpers._
  def evalCartesian(cartouches: Map[CPathField, (CPathField, List[ScalarStage.Focused])]): Option[List[Aggregator]] = {
    val interpreter = new MongoInterpreter(Version.$objectToArray, "root")
    Cartesian("root", Version.$objectToArray, cartouches, interpreter)
  }
  "empty cartesian should erase everything" >> {
    val actual = evalCartesian(Map.empty)
    val expected = Some(List(Aggregator.filter(E.Object("root_cartesian_empty" -> E.Bool(false)))))
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
      Aggregator.project(E.Object(
        "a" -> (E.key("root") +/ E.key("a")),
        "ba" -> (E.key("root") +/ E.key("b")),
        "bm" -> (E.key("root") +/ E.key("b")))),
      Aggregator.project(E.Object(
        "a" -> E.key("a"),
        "ba" -> cond(
          equal(typeExpr(E.key("ba")), E.String("array")),
          E.key("ba"),
          E.key("ba_non_existent_field")),
        "bm" -> E.key("bm"))),
      Aggregator.project(E.Object(
        "a" -> E.key("a"),
        "ba" -> E.key("ba"),
        "bm" -> E.key("bm"),
        "ba_unwind" -> E.key("ba"))),
      Aggregator.unwind(E.key("ba_unwind"), "ba_unwind_index"),
      Aggregator.project(E.Object(
        "a" -> E.key("a"),
        "ba" -> E.key("ba_unwind"),
        "bm" -> E.key("bm"))),
      Aggregator.project(E.Object(
        "a" -> E.key("a"),
        "ba" -> E.key("ba"),
        "bm" -> cond(
          equal(typeExpr(E.key("bm")), E.String("object")),
          E.key("bm"),
          E.key("bm_non_existent_field")))),
      Aggregator.project(E.Object(
        "a" -> E.key("a"),
        "ba" -> E.key("ba"),
        "bm" -> E.key("bm"),
        "bm_unwind" -> E.Object("$objectToArray" -> E.key("bm")))),
      Aggregator.unwind(E.key("bm_unwind"), "bm_unwind_index"),
      Aggregator.project(E.Object(
        "a" -> E.key("a"),
        "ba" -> E.key("ba"),
        "bm" -> (E.key("bm_unwind") +/ E.key("v")))),
      Aggregator.project(E.Object(
        "root" -> E.Object(
          "a" -> E.key("a"),
          "ba" -> E.key("ba"),
          "bm" -> E.key("bm")))),
      Aggregator.notNull("root")))

    val expectedDocs = expected map (_ map (_.toDocument))
    val actualDocs = actual map (_ map (_.toDocument))
    expectedDocs === actualDocs
  }
}
