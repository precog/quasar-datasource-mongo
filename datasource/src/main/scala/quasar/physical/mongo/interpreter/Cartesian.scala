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
import quasar.physical.mongo.expression._

import cats._
import cats.implicits._
import cats.mtl.MonadState
import higherkindness.droste.Basis

object Cartesian {
  def apply[F[_]: Monad: MonoidK: MonadInState, U: Basis[Expr, ?]](
      inner: Interpreter[F, U, ScalarStage])
      : Interpreter[F, U, ScalarStage.Cartesian] = new Interpreter[F, U, ScalarStage.Cartesian] {
    def apply(s: ScalarStage.Cartesian): F[List[Pipeline[U]]] =
      optToAlternative[F].apply(Projection.safeCartouches(s.cartouches)).flatMap { cartouches =>
        MonadState[F, InterpretationState].get flatMap { state =>
          val undefinedKey = state.uniqueKey.concat("_cartesian_empty")

          if (cartouches.isEmpty)
            unfocus[F] as List(Pipeline.Match(o.obj(Map(undefinedKey -> o.bool(false)))))
          else {

            val ns = x => state.uniqueKey.concat(x)

            val interpretations: F[List[List[Pipeline[U]]]] =
              cartouches.toList.traverse {
                case (alias, (_, instructions)) => for {
                  _ <- MonadState[F, InterpretationState].set(InterpretationState(ns(alias.name), Mapper.Focus(ns(alias.name))))
                  res <- instructions foldMapM { x => for {
                    a <- inner(x)
                    _ <- focus[F]
                  } yield a }
                } yield res
              }
            interpretations map { is =>
              val defaultObject = cartouches map {
                case (alias, _) => ns(alias.name) -> o.str("$".concat(ns(alias.name)))
              }

              val initialProjection: Pipeline[U] =
                Pipeline.Project(
                  Map("_id" -> o.int(0)) ++ (cartouches.map {
                    case (alias, (field, instructions)) =>
                      ns(alias.name) -> o.projection(state.mapper.projection(Projection.key(field.name)))
                  }))

              val instructions = is.flatMap(_.flatMap( {
                case Pipeline.Project(mp) =>
                  List(Pipeline.Project(defaultObject ++ mp))
                case Pipeline.Presented =>
                  List()
                case Pipeline.Erase =>
                  List()
                case x =>
                  List(x)
              }))

              val removeEmptyFields = Pipeline.Project { cartouches map {
                case (alias, _) => alias.name -> o.cond(
                  o.eqx(List(
                    o.str("$".concat(ns(alias.name))),
                    missing[Expr, U](ns(alias.name)))),
                  missingKey[Expr, U](ns(alias.name)),
                  o.str("$".concat(ns(alias.name))))
              }}

              val removeEmptyObjects =
                Pipeline.Match(o.or(cartouches.toList map {
                  case (k, v) => o.obj(Map(k.name -> o.exists(o.bool(true))))
                }))

              List(initialProjection) ++ instructions ++ List(removeEmptyFields, removeEmptyObjects)
            } flatMap { pipes =>
              unfocus[F] as pipes
            }
          }
        }
      }
  }
}
