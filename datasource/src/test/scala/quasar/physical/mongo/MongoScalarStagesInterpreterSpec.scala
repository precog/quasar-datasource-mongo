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

package quasar.physical.mongo

import slamdata.Predef._

import org.bson.{Document => _, _}

import quasar.api.table.ColumnType
import quasar.common.{CPath, CPathField}
import quasar.{IdStatus, ScalarStageSpec => Spec, ScalarStage, ScalarStages}

class MongoScalarStagesInterpreterSpec
    extends Spec.WrapSpec
    with Spec.ProjectSpec
    with Spec.MaskSpec
    with Spec.PivotSpec
    with Spec.FocusedSpec
    with Spec.CartesianSpec
    with StageInterpreterSpec {
  val wrapPendingExamples: Set[Int] = Set()
  val projectPendingExamples: Set[Int] = Set()
  val maskPendingExamples: Set[Int] = Set()
  val pivotPendingExamples: Set[Int] = Set()
  val focusedPendingExamples: Set[Int] = Set()
  val cartesianPendingExamples: Set[Int] = Set()

  "Id statuses" >> {
    val input = ldjson("""
      {"_id": "0", "value": "foo"}
      {"_id": "1", "value": "bar"}
      {"_id": "2", "value": "baz"}""")
    "ExcludeId" >> {
      val actual = interpret(ScalarStages(IdStatus.ExcludeId, List()), input, (x => x))
      actual must bestSemanticEqual(input)
    }
    "IdOnly" >> {
      val expected = ldjson("""
        "0"
        "1"
        "2"""")
      val actual = interpret(ScalarStages(IdStatus.IdOnly, List()), input, (x => x))
      actual must bestSemanticEqual(expected)
    }
    "IncludeId" >> {
      val expected = ldjson("""
        ["0", {"_id": "0", "value": "foo"}]
        ["1", {"_id": "1", "value": "bar"}]
        ["2", {"_id": "2", "value": "baz"}]""")
      val actual = interpret(ScalarStages(IdStatus.IncludeId, List()), input, (x => x))
      actual must bestSemanticEqual(expected)
    }
  }

  "Special its" >> {
    "multilevelFlatten" >> {
      val input = ldjson("""
        {"topArr": [[[4, 5, 6], {"a": "d", "b": "e", "c": "f"}], {"botArr":[7, 8, 9], "botObj": {"a": "g", "b": "h", "c": "i"}}], "topObj": {"midArr":[[10, 11, 12], {"a": "j", "b": "k", "c": "l"}], "midObj": {"botArr":[13, 14, 15], "botObj": {"a": "m", "b": "n", "c": "o"}}}}""")
      val stages = ScalarStages(IdStatus.ExcludeId, List(
        Project(CPath.parse("topObj")),
        Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
        Pivot(IdStatus.IncludeId, ColumnType.Object),
        Mask(Map(
          CPath.parse("[0]") -> ColumnType.Top,
          CPath.parse("[1]") -> ColumnType.Top)),
        Mask(Map(
          CPath.parse("[0]") -> ColumnType.Top,
          CPath.parse("[1]") -> Set(ColumnType.Object))),
        Wrap("cartesian"),
        Cartesian(Map(
          (CPathField("cartouche0"), (CPathField("cartesian"), List(Project(CPath.parse("[0]"))))),
          (CPathField("cartouche1"), (CPathField("cartesian"), List(
            Project(CPath.parse("[1]")),
            Pivot(IdStatus.IncludeId, ColumnType.Object),
            Mask(Map(CPath.parse("[0]") -> ColumnType.Top, CPath.parse("[1]") -> ColumnType.Top))))))),
        Cartesian(Map(
          (CPathField("k1"), (CPathField("cartouche0"), List())),
          (CPathField("v2"), (CPathField("cartouche1"), List(Project(CPath.parse("[1]"))))),
          (CPathField("k2"), (CPathField("cartouche1"), List(Project(CPath.parse("[0]")))))))))

      val expected = ldjson("""
        { "k1": "midArr" }
        { "k1": "midObj", "k2": "botArr", "v2": [13, 14, 15] }
        { "k1": "midObj", "k2": "botObj", "v2": {"a": "m", "b": "n", "c": "o" } }
      """)
      val actual = interpret(stages, input, (x => x))
      actual must bestSemanticEqual(expected)
    }
    "arrayLengthHeterogeneous" >> {
      val input = ldjson("""
        { "a": 1, "b": { "x": 42, "y": 21 } }
        { "a": 2, "b": [ "u", "v"] }
        { "a": 3, "b": {} }
        { "a": 4, "b": [] }
        { "a": 5, "b": { "z": "string" } }
        { "a": 6, "b": [ "w"] }
        { "a": 7, "b": "string" }
        { "a": 8, "b": null }
        { "a": 9, "b": { "d": [1, 2, 3], "e": { "n": 1}, "f": null, "g": "foo", "h": {}, "i": [] }}
        { "a": 10, "b": [[4, 5, 6], { "m": 1}, null, "foo", {}, []] }""")

      val stages = ScalarStages(IdStatus.ExcludeId, List(
        Mask(Map(
          (CPath.parse("a"), ColumnType.Top),
          (CPath.parse("b"), ColumnType.Top)))))

      val expected = input

      val actual = interpret(stages, input, (x => x))
      actual must bestSemanticEqual(expected)
    }
    "heterogeneousShiftArrayValueAndShiftMapValueWithField" >> {
      val input = ldjson("""
        { "a": 1, "b": [ ] }
        { "a": 2, "b": { } }""")
      val stages = ScalarStages(IdStatus.ExcludeId, List(
        Cartesian(Map(
          (CPathField("a"), (CPathField("a"), List())),
          (CPathField("ba"), (CPathField("b"), List(
            Mask(Map(CPath.Identity -> Set(ColumnType.Array))),
            Pivot(IdStatus.ExcludeId, ColumnType.Array))))))))
      val expected = ldjson("""
        {"a": 1}
        {"a": 2}""")

      val actual = interpret(stages, input, (x => x))
      actual must bestSemanticEqual(expected)

    }
  }

  val RootKey: String = "rootKey"

  val RootProjection = Project(CPath.parse(".rootKey"))

  def rootWrapper(b: JsonElement): JsonElement = new BsonDocument(RootKey, b)

  def evalFocused(focused: List[ScalarStage.Focused], stream: JsonStream): JsonStream =
    interpret(ScalarStages(IdStatus.ExcludeId, RootProjection :: focused), stream, rootWrapper)

  def evalOneStage(stage: ScalarStage, stream: JsonStream): JsonStream =
    interpret(ScalarStages(IdStatus.ExcludeId, List(RootProjection, stage)), stream, rootWrapper)

  def evalWrap(wrap: Wrap, stream: JsonStream): JsonStream =
    evalOneStage(wrap, stream)

  def evalProject(project: Project, stream: JsonStream): JsonStream =
    evalOneStage(project, stream)

  def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream =
    evalOneStage(pivot, stream)

  def evalMask(mask: Mask, stream: JsonStream): JsonStream =
    evalOneStage(mask, stream)

  def evalCartesian(cartesian: Cartesian, stream: JsonStream): JsonStream =
    evalOneStage(cartesian, stream)
}
