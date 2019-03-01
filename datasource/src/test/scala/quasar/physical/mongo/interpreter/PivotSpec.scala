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

class PivotSpec extends Specification {
/*
  "Array examples" >> {
    def mkExpected(e: Expr): Option[List[Pipe]] = Some(List(
      Pipeline.$project(Map("root_unwind" -> O.key("root"))),
      Pipeline.$unwind("root_unwind", "root_unwind_index"),
      Pipeline.$project(Map("root" -> e)),
      Pipeline.NotNull("root")))

    "id only" >> {
      val actual = Pivot("root", IdStatus.IdOnly, ColumnType.Array)
      val expected = mkExpected(O.key("root_unwind_index"))
      actual === expected
    }
    "values only" >> {
      val actual = Pivot("root", IdStatus.ExcludeId, ColumnType.Array)
      val expected = mkExpected(O.key("root_unwind"))
      actual === expected
    }
    "both" >> {
      val actual = Pivot("root", IdStatus.IncludeId, ColumnType.Array)
      val expected = mkExpected(O.array(List(O.key("root_unwind_index"), O.key("root_unwind"))))
      actual === expected
    }
  }

  "Object examples" >> {
    def mkExpected(e: Expr): Option[List[Pipe]] = Some(List(
      Pipeline.$project(Map("root_unwind" -> O.$objectToArray(O.key("root")))),
      Pipeline.$unwind("root_unwind", "root_unwind_index"),
      Pipeline.$project(Map("root" -> e)),
      Pipeline.NotNull("root")))

    "id only" >> {
      val actual = Pivot("root", IdStatus.IdOnly, ColumnType.Object)
      val expected = mkExpected(O.projection(Projection.key("root_unwind") + Projection.key("k")))
      actual === expected
    }
    "values only" >> {
      val actual = Pivot("root", IdStatus.ExcludeId, ColumnType.Object)
      val expected = mkExpected(O.projection(Projection.key("root_unwind") + Projection.key("v")))
      actual === expected
    }
    "both" >> {
      val actual = Pivot("root", IdStatus.IncludeId, ColumnType.Object)
      val expected = mkExpected(O.array(List(
        O.projection(Projection.key("root_unwind") + Projection.key("k")),
        O.projection(Projection.key("root_unwind") + Projection.key("v")))))

      actual === expected
    }
  }
 */
}
