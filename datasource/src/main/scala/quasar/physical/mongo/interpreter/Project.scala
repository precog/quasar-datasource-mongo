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

package quasar.physical.mongo.interpreter

import slamdata.Predef._

import quasar.physical.mongo.expression._
import quasar.physical.mongo.expression.Projection.Step

import scalaz.{MonadState, Scalaz}, Scalaz._

object Project {
  def stepType(step: Step): Expr = step match {
    case Step.Field(_) => O.string("object")
    case Step.Index(_) => O.string("array")
  }

  // We need to be sure that type of projectee is coherent with projector
  // E.g. if we project [1] the type of Projection(List()) must be "array"
  @scala.annotation.tailrec
  def build(
      key: String,
      prj: Projection,
      input: Field,
      output: Field,
      res: Field,
      acc: List[Pipe])
      : List[Pipe] = prj.steps match {

    case List() =>
      acc ++ List(
        Pipeline.$project(Map(res.keyPart -> O.steps(List(input)))),
        Pipeline.Presented)
    case hd :: tail =>
      val projectionObject =
        O.$cond(
          O.$or(List(
            O.$not(O.$eq(List(O.$type(O.steps(List(input))), stepType(hd)))),
            O.$eq(List(O.$type(O.steps(List(input, hd))), O.string("missing"))))),
          missing(key),
          O.steps(List(input, hd)))
      val project: Pipe = Pipeline.$project(Map(output.keyPart -> projectionObject))
      build(key, Projection(tail), output, input, res, acc ++ List(project, Pipeline.Presented))
  }

  def apply[F[_]: MonadInState](prj: Projection): F[List[Pipe]] =
    MonadState[F, InterpretationState].get map { state =>
      val tmpKey0 = state.uniqueKey concat "_project0"
      val tmpKey1 = state.uniqueKey concat "_project1"
      val fld = Mapper.projection(state.mapper)(prj)
      val initialProjection = Pipeline.$project(Map(tmpKey0 -> O.steps(List())))
      initialProjection :: build(state.uniqueKey, fld, Field(tmpKey0), Field(tmpKey1), Field(state.uniqueKey), List())
    } flatMap { a => focus[F] as a }
}
