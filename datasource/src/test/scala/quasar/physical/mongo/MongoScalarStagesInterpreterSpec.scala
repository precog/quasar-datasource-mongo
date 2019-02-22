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

import quasar.common.CPath
import quasar.{IdStatus, ScalarStageSpec => Spec, JsonSpec, ScalarStage, ScalarStages}, ScalarStage._

class MongoScalarStagesInterpreterSpec
    extends JsonSpec
//    with Spec.WrapSpec
//    with Spec.ProjectSpec
//    with Spec.PivotSpec
    with Spec.MaskSpec
//    with Spec.CartesianSpec
    with StageInterpreterSpec {

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

  val RootKey: String = "rootKey"

  val RootProjection = Project(CPath.parse(".rootKey"))

  def rootWrapper(b: JsonElement): JsonElement = new BsonDocument(RootKey, b)

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
