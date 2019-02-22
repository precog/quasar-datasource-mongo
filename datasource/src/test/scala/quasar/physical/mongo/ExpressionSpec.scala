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

import quasar.{RenderTree, TreeMatchers}, RenderTree.ops._
import scalaz.syntax.show._

import matryoshka.birecursiveIso
import matryoshka.data.Fix
import quasar.physical.mongo.{Expression, Optics, CustomPipeline, MongoPipeline, Pipeline, Projection, Step, Field, Index}, Expression._

class ExpressionSpec extends Specification with TreeMatchers {
  val O = Optics.full(birecursiveIso[Fix[Projected], Projected].reverse.asPrism)

  "let with index" >> {
    val proj = Projection(List()) + Projection.key("a") + Projection.index(0) + Projection.index(1)
    println(proj.render.shows)
    val obj = Expression.unfoldProjection[Fix](proj)
    println(obj.render.shows)
    true
  }
}
