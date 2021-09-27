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

package quasar.physical.mongo

import slamdata.Predef._

import cats.data.NonEmptyList
import cats.implicits._

import org.bson.{Document => _, _}

import quasar.api.ColumnType
import quasar.api.push.InternalKey
import quasar.common.{CPath, CPathField}
import quasar.{IdStatus, ScalarStageSpec => Spec, ScalarStage, ScalarStages}

import java.time.{OffsetDateTime, ZoneOffset}

import skolems.∃

import spire.math.Real

import org.specs2.matcher.Matcher

class MongoScalarStagesInterpreterSpec
    extends Spec.WrapSpec
    with Spec.ProjectSpec
    with Spec.MaskSpec
    with Spec.PivotSpec
    with Spec.FocusedSpec
    with Spec.CartesianSpec
    with Spec.FullSpec
    with StageInterpreterSpec {
  val wrapPendingExamples: Set[Int] = Set()
  val projectPendingExamples: Set[Int] = Set()
  val maskPendingExamples: Set[Int] = Set()
  val pivotPendingExamples: Set[Int] = Set()
  val focusedPendingExamples: Set[Int] = Set()
  val cartesianPendingExamples: Set[Int] = Set()
  val fullPendingExamples: Set[Int] = Set()

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
    "realGiraffe" >> {
      val input = ldjson("""
        {"b2fe01ea-a7e0-452c-95e6-7047a62ecc71":{"S":{"to":["ethan@example.com"],"rId":"2fe76790"},"dateTime":1518610539149.4202, "testField":"A"}, "X": "P"}
        {"f5fb62c9-564d-4c3f-b0a5-a804a3cc4d25":{"S":{"to":["jacques@example.com"],"rId":"8e820358"},"dateTime":1518610554083.7378, "testField":"B"}, "X": "Q"}
        {"d153fccb-1707-42e3-ba90-03c473687964":{"S":{"to":["deshawn@example.com"],"rId":"28642fbe"},"dateTime":1518610722075.8232, "testField":"C"}, "X": "R"}
        {"b5207e48-10b4-4a42-8e6e-9a4551a88249":{"MAS":{"sId":"661929ef","rId":"08a42ad4"},"dateTime":1518610493869.1462, "testField":"D"}, "X": "S"}
        {"cfc2c0d5-b81e-4f3c-9bf4-d6d06e4ba82f":{"MAS":{"sId":"c12c935b","rId":"0b609f84"},"dateTime":1518610653487.1868, "testField":"E"}, "X": "T"}
        {"shifted": false}
        {"shifted": {"shifted": true}}
        {}
        []
        [3, 5, 7, 11]
        123.456""")

      val expected = ldjson("""
        {"key":"X","x":"P"}
        {"testField":"A","key":"b2fe01ea-a7e0-452c-95e6-7047a62ecc71","x":"P"}
        {"key":"X","x":"Q"}
        {"testField":"B","key":"f5fb62c9-564d-4c3f-b0a5-a804a3cc4d25","x":"Q"}
        {"key":"X","x":"R"}
        {"testField":"C","key":"d153fccb-1707-42e3-ba90-03c473687964","x":"R"}
        {"key":"X","x":"S"}
        {"testField":"D","key":"b5207e48-10b4-4a42-8e6e-9a4551a88249","x":"S"}
        {"key":"X","x":"T"}
        {"testField":"E","key":"cfc2c0d5-b81e-4f3c-9bf4-d6d06e4ba82f","x":"T"}
        {"key":"shifted"}
        {"key":"shifted"}""")

      val stages = ScalarStages(IdStatus.ExcludeId, List(
        RootProjection,
        Wrap("cartesian"),
        Cartesian(Map(
          (CPathField("cartouche1"), (CPathField("cartesian"), List(Project(CPath.parse("X"))))),
          (CPathField("cartouche0"), (CPathField("cartesian"), List(
            Mask(Map(CPath.Identity -> Set(ColumnType.Object))),
            Pivot(IdStatus.IncludeId, ColumnType.Object),
            Mask(Map(
              (CPath.parse("[1].testField"), ColumnType.Top),
              (CPath.parse("[0]"), ColumnType.Top)))))))),
        Cartesian(Map(
          (CPathField("key"), (CPathField("cartouche0"), List(Project(CPath.parse("[0]"))))),
          (CPathField("testField"), (CPathField("cartouche0"), List(Project(CPath.parse("[1].testField"))))),
          (CPathField("x"), (CPathField("cartouche1"), List()))))))

      val actual = interpret(stages, input, rootWrapper)
      actual must bestSemanticEqual(expected)
    }
  }

  "Seeking" >> {
    "with pushdown" >> {
      "on datetime" >> {
        val input = ldjson("""
          {"foo": ["a", "b"], "bar": [1, 2] , "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-20T10:50:33.592Z" } } } }
          {"foo": ["c", "d"], "bar": [3, 4] , "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-21T10:50:33.592Z" } } } }
          {"foo": ["e", "f"], "bar": [5, 6] , "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-22T10:50:33.592Z" } } } }
          {"foo": ["g", "h"], "bar": [7, 8] , "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-23T10:50:33.592Z" } } } }
          """)

        val expected = ldjson("""
          {"foo": "e", "bar": 5, "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-22T10:50:33.592Z" } } } }
          {"foo": "e", "bar": 6, "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-22T10:50:33.592Z" } } } }
          {"foo": "f", "bar": 5, "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-22T10:50:33.592Z" } } } }
          {"foo": "f", "bar": 6, "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-22T10:50:33.592Z" } } } }
          {"foo": "g", "bar": 7, "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-23T10:50:33.592Z" } } } }
          {"foo": "g", "bar": 8, "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-23T10:50:33.592Z" } } } }
          {"foo": "h", "bar": 7, "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-23T10:50:33.592Z" } } } }
          {"foo": "h", "bar": 8, "baz": {"qux": { "ts": { "$offsetdatetime": "2020-07-23T10:50:33.592Z" } } } }
          """)

        val after = OffsetDateTime.of(2020, 7, 22, 5, 0, 0, 0, ZoneOffset.UTC)
        val offset = MongoOffset(
          NonEmptyList.of("baz".asLeft, "qux".asLeft, "ts".asLeft),
          ∃(InternalKey.Actual.dateTime(after))).toOption
        val stages =
          ScalarStages(
            IdStatus.ExcludeId,
            List(
              ScalarStage.Mask(
                Map(
                  CPath.parse("foo") -> Set(ColumnType.Array),
                  CPath.parse("bar") -> Set(ColumnType.Array),
                  CPath.parse("baz") -> ColumnType.Top)),
              ScalarStage.Cartesian(
                Map(
                  CPathField("foo") -> (CPathField("foo") -> List(Pivot(IdStatus.ExcludeId, ColumnType.Array))),
                  CPathField("bar") -> (CPathField("bar") -> List(Pivot(IdStatus.ExcludeId, ColumnType.Array))),
                  CPathField("baz") -> (CPathField("baz") -> List.empty)))))

        val actual = interpretWithOffset(stages, input, offset, x => x)

        actual must bestSemanticEqualNoId(expected)
      }
    }

    "without pushdown" >> {
      "on datetime" >> {
        val input = ldjson("""
          {"foo": 1, "bar": {"qux": { "ts": { "$offsetdatetime": "2020-07-20T10:50:33.592Z" } } } }
          {"foo": 2, "bar": {"qux": { "ts": { "$offsetdatetime": "2020-07-21T10:50:33.592Z" } } } }
          {"foo": 3, "bar": {"qux": { "ts": { "$offsetdatetime": "2020-07-22T10:50:33.592Z" } } } }
          {"foo": 4, "bar": {"qux": { "ts": { "$offsetdatetime": "2020-07-23T10:50:33.592Z" } } } }
          """)

        val expected = ldjson("""
          {"foo": 3, "bar": {"qux": { "ts": { "$offsetdatetime": "2020-07-22T10:50:33.592Z" } } } }
          {"foo": 4, "bar": {"qux": { "ts": { "$offsetdatetime": "2020-07-23T10:50:33.592Z" } } } }
          """)

        val after = OffsetDateTime.of(2020, 7, 22, 5, 0, 0, 0, ZoneOffset.UTC)
        val offset = MongoOffset(
          NonEmptyList.of("bar".asLeft, "qux".asLeft, "ts".asLeft),
          ∃(InternalKey.Actual.dateTime(after))).toOption

        val actual = interpretWithOffset(ScalarStages.Id, input, offset, x => x)

        actual must bestSemanticEqualNoId(expected)
      }

      "on integer" >> {
        val input = ldjson("""
          {"foo": 1, "bar": {"qux": { "ix": ["a", 1] } } }
          {"foo": 2, "bar": {"qux": { "ix": ["b", 2] } } }
          {"foo": 3, "bar": {"qux": { "ix": ["c", 3] } } }
          {"foo": 4, "bar": {"qux": { "ix": ["d", 4] } } }
          """)

        val expected = ldjson("""
          {"foo": 3, "bar": {"qux": { "ix": ["c", 3] } } }
          {"foo": 4, "bar": {"qux": { "ix": ["d", 4] } } }
          """)

        val offset = MongoOffset(
          NonEmptyList.of("bar".asLeft, "qux".asLeft, "ix".asLeft, 1.asRight),
          ∃(InternalKey.Actual.real(Real(3)))).toOption

        val actual = interpretWithOffset(ScalarStages.Id, input, offset, x => x)

        actual must bestSemanticEqualNoId(expected)
      }

      "on long" >> {
        val startId = Int.MaxValue.toLong + 1

        val input = ldjson(s"""
          {"foo": 1, "bar": {"qux": { "ix": ["a", ${startId + 1}] } } }
          {"foo": 2, "bar": {"qux": { "ix": ["b", ${startId + 2}] } } }
          {"foo": 3, "bar": {"qux": { "ix": ["c", ${startId + 3}] } } }
          {"foo": 4, "bar": {"qux": { "ix": ["d", ${startId + 4}] } } }
          """)

        val expected = ldjson(s"""
          {"foo": 3, "bar": {"qux": { "ix": ["c", ${startId + 3}] } } }
          {"foo": 4, "bar": {"qux": { "ix": ["d", ${startId + 4}] } } }
          """)

        val offset = MongoOffset(
          NonEmptyList.of("bar".asLeft, "qux".asLeft, "ix".asLeft, 1.asRight),
          ∃(InternalKey.Actual.real(Real(startId + 3)))).toOption

        val actual = interpretWithOffset(ScalarStages.Id, input, offset, x => x)

        actual must bestSemanticEqualNoId(expected)
      }

      "on float" >> {
        val input = ldjson("""
          {"foo": 1, "bar": {"qux": { "ix": ["a", 0.1] } } }
          {"foo": 2, "bar": {"qux": { "ix": ["b", 0.2] } } }
          {"foo": 3, "bar": {"qux": { "ix": ["c", 0.3] } } }
          {"foo": 4, "bar": {"qux": { "ix": ["d", 0.4] } } }
          """)

        val expected = ldjson("""
          {"foo": 3, "bar": {"qux": { "ix": ["c", 0.3] } } }
          {"foo": 4, "bar": {"qux": { "ix": ["d", 0.4] } } }
          """)

        val offset = MongoOffset(
          NonEmptyList.of("bar".asLeft, "qux".asLeft, "ix".asLeft, 1.asRight),
          ∃(InternalKey.Actual.real(Real(0.3)))).toOption

        val actual = interpretWithOffset(ScalarStages.Id, input, offset, x => x)

        actual must bestSemanticEqualNoId(expected)
      }

      "on string" >> {
        val input = ldjson("""
          {"foo": 1, "bar": {"qux": { "ix": ["a", "aaa"] } } }
          {"foo": 2, "bar": {"qux": { "ix": ["b", "aab"] } } }
          {"foo": 3, "bar": {"qux": { "ix": ["c", "aac"] } } }
          {"foo": 4, "bar": {"qux": { "ix": ["d", "aad"] } } }
          """)

        val expected = ldjson("""
          {"foo": 3, "bar": {"qux": { "ix": ["c", "aac"] } } }
          {"foo": 4, "bar": {"qux": { "ix": ["d", "aad"] } } }
          """)

        val offset = MongoOffset(
          NonEmptyList.of("bar".asLeft, "qux".asLeft, "ix".asLeft, 1.asRight),
          ∃(InternalKey.Actual.string("aac"))).toOption

        val actual = interpretWithOffset(ScalarStages.Id, input, offset, x => x)

        actual must bestSemanticEqualNoId(expected)
      }
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

  def evalFull(stages: ScalarStages, stream: JsonStream): JsonStream =
    interpret(stages, stream, (x => x))

  def bestSemanticEqualNoId(js: JsonStream): Matcher[JsonStream] =
    bestSemanticEqual(js) ^^ ((jsons: JsonStream) => jsons map {
      case (doc: BsonDocument) => {
        doc.remove("_id")

        doc
      }
      case otherwise => otherwise
    })
}
