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

import quasar.ScalarStage
import quasar.physical.mongo.Interpreter
import quasar.physical.mongo.expression._, Projection._, Step._

import cats.{Monad, MonoidK}
import cats.implicits._
import cats.mtl.MonadState
import higherkindness.droste.Basis

object Project {
  def apply[F[_]: Monad: MonadInState: MonoidK, U: Basis[Expr, ?]]: Interpreter[F, U, ScalarStage.Project] =
    new Interpreter[F, U, ScalarStage.Project] {
      def apply(s: ScalarStage.Project): F[List[Pipeline[U]]] =
        optToAlternative[F].apply(Projection.fromCPath(s.path)) flatMap { (prj: Projection) =>
          MonadState[F, InterpretationState].get map { state =>
            val tmpKey0 = state.uniqueKey.concat("_project0")
            val tmpKey1 = state.uniqueKey.concat("_project1")
            val fld = state.mapper.projection(prj)
            val initialProjection = Pipeline.Project(Map(tmpKey0 -> o.projection(Projection(List()))))
            initialProjection :: build(state.uniqueKey, fld, Field(tmpKey0), Field(tmpKey1), Field(state.uniqueKey), List())
          } flatMap { a => focus[F] as a }
        }

      def stepType: Step => U = {
        case Step.Field(_) => o.str("object")
        case Step.Index(_) => o.str("array")
      }

      @scala.annotation.tailrec
      def build(key: String, prj: Projection, input: Field, output: Field, res: Field, acc: List[Pipeline[U]])
          : List[Pipeline[U]] = {
        prj.steps match {
          case List() =>
            acc ++ List(
              Pipeline.Project(Map(res.keyPart -> o.projection(Projection(List(input))))),
              Pipeline.Presented)
          case hd :: tail =>
            val projectionObject =
              o.cond(
                o.or(List(
                  o.not(o.eqx(List(o.typ(o.projection(Projection(List(input)))), stepType(hd)))),
                  o.eqx(List(o.typ(o.projection(Projection(List(input, hd)))), o.str("missing"))))),
                missing[Expr, U](key),
                o.projection(Projection(List(input, hd))))
            val project: Pipeline[U] = Pipeline.Project(Map(output.keyPart -> projectionObject))
            build(key, Projection(tail), output, input, res, acc ++ List(project, Pipeline.Presented))
        }
      }
    }
}
