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
import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Version, Aggregator, MongoExpression => E}
import quasar.IdStatus

class PivotSpec extends Specification {
  "Min versions" >> {
    "Array and too old mongo" >> {
      Pivot("root", Version.zero, IdStatus.IdOnly, ColumnType.Array) must beNone
    }
    "Array and mongo with unwind.includeArrayIndices" >> {
      Pivot("root", Version.$unwind, IdStatus.IdOnly, ColumnType.Array) must beSome
    }
    "Object and too old mongo" >> {
      Pivot("root", Version.$unwind, IdStatus.IdOnly, ColumnType.Object) must beNone
    }
    "Object and mongo with $objectToArray" >> {
      Pivot("root", Version.$objectToArray, IdStatus.IdOnly, ColumnType.Object) must beSome
    }
  }
  "Array examples" >> {
    def mkExpected(e: MongoExpression): Option[List[Aggregator]] = Some(List(
      Aggregator.project(E.Object("root_unwind" -> E.key("root"))),
      Aggregator.unwind(E.key("root_unwind"), "root_unwind_index"),
      Aggregator.project(E.Object("root" -> e)),
      Aggregator.notNull("root")))

    "id only" >> {
      val actual = Pivot("root", Version.$objectToArray, IdStatus.IdOnly, ColumnType.Array)
      val expected = mkExpected(E.key("root_unwind_index"))
      actual === expected
    }
    "values only" >> {
      val actual = Pivot("root", Version.$objectToArray, IdStatus.ExcludeId, ColumnType.Array)
      val expected = mkExpected(E.key("root_unwind"))
      actual === expected
    }
    "both" >> {
      val actual = Pivot("root", Version.$objectToArray, IdStatus.IncludeId, ColumnType.Array)
      val expected = mkExpected(E.Array(E.key("root_unwind_index"), E.key("root_unwind")))
      actual === expected
    }
  }

  "Object examples" >> {
    def mkExpected(e: MongoExpression): Option[List[Aggregator]] = Some(List(
      Aggregator.project(E.Object("root_unwind" -> E.Object("$objectToArray" -> E.key("root")))),
      Aggregator.unwind(E.key("root_unwind"), "root_unwind_index"),
      Aggregator.project(E.Object("root" -> e)),
      Aggregator.notNull("root")))

    "id only" >> {
      val actual = Pivot("root", Version.$objectToArray, IdStatus.IdOnly, ColumnType.Object)
      val expected = mkExpected(E.key("root_unwind") +/ E.key("k"))
      actual === expected
    }
    "values only" >> {
      val actual = Pivot("root", Version.$objectToArray, IdStatus.ExcludeId, ColumnType.Object)
      val expected = mkExpected(E.key("root_unwind") +/ E.key("v"))
      actual === expected
    }
    "both" >> {
      val actual = Pivot("root", Version.$objectToArray, IdStatus.IncludeId, ColumnType.Object)
      val expected = mkExpected(E.Array(E.key("root_unwind") +/ E.key("k"), E.key("root_unwind") +/ E.key("v")))
      actual === expected
    }
  }
}
