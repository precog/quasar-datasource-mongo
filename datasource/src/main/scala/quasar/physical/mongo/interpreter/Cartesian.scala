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

import quasar.physical.mongo.expression._
import quasar.ScalarStage

import scalaz.{MonadState, Scalaz, PlusEmpty}, Scalaz._

import shims._

object Cartesian {
  def apply[F[_]: MonadInState: PlusEmpty](
      cartouches: Map[Field, (Field, List[ScalarStage.Focused])],
      interpretStep: ScalarStage => F[List[Pipe]])
      : F[List[Pipe]] =

  MonadState[F, InterpretationState].get flatMap { state =>
    val undefinedKey = state.uniqueKey concat "_cartesian_empty"

    if (cartouches.isEmpty)
      unfocus[F] as List(Pipeline.$match(O.obj(Map(undefinedKey -> O.bool(false)))): Pipe)
    else {
      val ns = x => state.uniqueKey concat x
      val interpretations: F[List[List[Pipe]]] =
        cartouches.toList.traverse {
          case (alias, (_, instructions)) => for {
            _ <- MonadState[F, InterpretationState].put(InterpretationState(ns(alias.name), Mapper.Focus(ns(alias.name))))
            res <- instructions foldMapM { x => for {
              a <- interpretStep(x)
              _ <- focus[F]
            } yield a }
          } yield res
        }

      interpretations map { is =>
        val defaultObject = cartouches map {
          case (alias, _) => ns(alias.name) -> O.string("$" concat ns(alias.name))
        }
        // We don't need to map any projections except initial since they're mapped already
        val initialProjection =
          Pipeline.$project(
            Map("_id" -> O.int(0)) ++ (cartouches map {
              case (alias, (field, instructions)) =>
                ns(alias.name) -> O.projection(Mapper.projection(state.mapper)(Projection.key(field.name)))
            }))

        val instructions = is flatMap (_ flatMap {
          case Pipeline.$project(mp) =>
            List(Pipeline.$project(defaultObject ++ mp))
          case Pipeline.Presented => List()
          case Pipeline.Erase => List()
          case x => List(x)
        })

        val removeEmptyFields =
          Pipeline.$project(cartouches map {
            case (alias, _) => alias.name -> O.$cond(
              O.$eq(List(
                O.string("$" concat ns(alias.name)),
                missing(ns(alias.name)))),
              missingKey(ns(alias.name)),
              O.string("$" concat ns(alias.name)))
          })

        val removeEmptyObjects =
          Pipeline.$match(O.$or(cartouches.toList map {
            case (k, v) => O.obj(Map(k.name -> O.$exists(O.bool(true))))
          }))
        initialProjection :: instructions ++ List(removeEmptyFields, removeEmptyObjects)
      } flatMap { pipes =>
        unfocus[F] as pipes
      }
    }
  }
}
