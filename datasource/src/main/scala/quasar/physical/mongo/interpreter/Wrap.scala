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
import quasar.contrib.iota._
import quasar.physical.mongo.Interpreter
import quasar.physical.mongo.expression._

import cats.{Monad, MonoidK}
import cats.implicits._
import cats.mtl.MonadState
import higherkindness.droste.Basis

object Wrap {
  def apply[F[_]: Monad: MonadInState: MonoidK, U: Basis[Expr, ?]]
      : Interpreter[F, U, ScalarStage.Wrap] =
    new Interpreter[F, U, ScalarStage.Wrap] {
      def apply(s: ScalarStage.Wrap): F[List[Pipeline[U]]] = optToAlternative[F].apply(Projection.safeField(s.name)) flatMap { name =>
        for {
          state <- MonadState[F, InterpretationState].get
          (res: List[Pipeline[U]]) = List(Pipeline.Project(Map(
            state.uniqueKey ->
              o.cond(
                o.eqx(List(o.projection(Projection(List())), missing[Expr, U](state.uniqueKey))),
                missing[Expr, U](state.uniqueKey),
                o.obj(Map(name.name -> o.projection(Projection(List()))))),
            "_id" -> o.int(0))))
          _ <- focus[F]
        } yield res.map(_.map(Compiler.mapProjection[Expr, U, U](state.mapper.projection)))
      }
    }
}
