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
      : F[List[Pipe]] = MonadState[F, InterpretationState].gets(_.uniqueKey) flatMap { uniqueKey =>

    val undefinedKey = uniqueKey.concat("_cartesian_empty")
    if (cartouches.isEmpty)
      MonadState[F, InterpretationState].point(List(Pipeline.$match(Map(undefinedKey -> O.bool(false))): Pipe))
    else {
      val cartoucheList = cartouches.toList

      val interpretations: F[List[List[Pipe]]] =
        cartoucheList.traverse {
          case (alias, (_, instructions)) =>
            instructions foldMapM { x => for {
              _ <- MonadState[F, InterpretationState].put(InterpretationState(alias.name, Mapper.Identity))
              res <- interpretStep(x)
            } yield res }
        }

      interpretations map { is =>
        val defaultPairs: List[(String, Expr)] =
          cartoucheList map {
            case (alias, _) => (alias.name, O.key(alias.name))
          }

        val defaultObject = Map(defaultPairs:_*)

        val initialProjectionPairs = cartoucheList map {
          case (alias, (field, instructions)) => alias.name -> O.projection(Projection.key(uniqueKey) + Projection.key(field.name))
        }

        val initialProjection =
          Pipeline.$project(Map(initialProjectionPairs:_*))

        val lastProjectionPairs = cartoucheList map {
          case (alias, _) => alias.name -> O.key(alias.name)
        }

        val lastProjection =
          Pipeline.$project(Map(uniqueKey -> O.obj(Map(lastProjectionPairs:_*))))

        val instructions = is flatMap (_ flatMap {
          case Pipeline.$project(mp) =>
            List(Pipeline.$project(defaultObject ++ mp))
          case Pipeline.NotNull(_) =>
            List()
          case x => List(x)
        })

        List(initialProjection) ++ instructions ++ List(lastProjection, Pipeline.NotNull(Projection.key(uniqueKey)))
      }
    }
  }
}
