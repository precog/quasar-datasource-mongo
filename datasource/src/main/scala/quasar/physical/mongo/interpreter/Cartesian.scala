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

import cats.syntax.foldable._
import cats.syntax.traverse._
import cats.instances.option._
import cats.instances.list._

import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Interpretation, Interpreter, Aggregator, Version, MongoExpression => E}
import quasar.ScalarStage

import shims._

import E.ProjectionStep._

object Cartesian {
  def apply(
      uniqueKey: String,
      version: Version,
      cartouches: Map[Field, (Field, List[ScalarStage.Focused])],
      interpreter: Interpreter)
      : Option[List[Aggregator]] = {

    val undefinedKey = uniqueKey.concat("_cartesian_empty")
    if (cartouches.isEmpty) Some(List(Aggregator.filter(E.Object(undefinedKey -> E.Bool(false)))))
    else {
      val cartoucheList = cartouches.toList

      val interpretations: Option[List[List[Aggregator]]] =
        cartoucheList.traverse {
          case (alias, (_, instructions)) =>
            val interpreted: Interpretation =
              interpreter.refineInterpretation(alias.name, Interpretation.initial(instructions))
            if (!interpreted.stages.isEmpty) None
            else Some(interpreted.aggregators)
        }

      interpretations map { is =>
        val defaultPairs: List[(String, MongoExpression)] =
          cartoucheList map {
            case (alias, _) => (alias.name, E.key(alias.name))
          }
        val defaultObject = E.Object(defaultPairs:_*)

        val initialProjectionPairs = cartoucheList map {
          case (alias, (field, instructions)) => alias.name -> (E.key(uniqueKey) +/ E.key(field.name))
        }

        val initialProjection =
          Aggregator.project(E.Object(initialProjectionPairs:_*))

        val lastProjectionPairs = cartoucheList map {
          case (alias, _) => alias.name -> E.key(alias.name)
        }
        val lastProjection = Aggregator.project(E.Object(uniqueKey -> E.Object(lastProjectionPairs:_*)))

        val instructions = is flatMap (_ flatMap {
          case Aggregator.project(a @ E.Object(_*)) =>
            List(Aggregator.project(defaultObject + a))
          case Aggregator.notNull(_) =>
            List()
          case x => List(x)
        })

        List(initialProjection) ++ instructions ++ List(lastProjection, Aggregator.notNull(uniqueKey))
      }
    }
  }

  import matryoshka.birecursiveIso
  import matryoshka.data.Fix
  import quasar.physical.mongo.{Expression, Optics, CustomPipeline, MongoPipeline, Pipeline, Projection, Step, Field, Index}, Expression._
  val O = Optics.full(birecursiveIso[Fix[Projected], Projected].reverse.asPrism)
  def apply0(uniqueKey: String, cartouches: Map[Field, (Field, List[ScalarStage.Focused])], interpreter: Interpreter)
      : Option[List[Pipeline[Fix[Projected]]]] = {

    val undefinedKey = uniqueKey.concat("_cartesian_empty")
    if (cartouches.isEmpty) Some(List(Pipeline.$match(Map(undefinedKey -> O.bool(true)))))
    else {
      val cartoucheList = cartouches.toList

      val interpretations: Option[List[List[Pipeline[Fix[Projected]]]]] =
        cartoucheList.traverse {
          case (alias, (_, instructions)) =>
            instructions foldMapM { x => interpreter.interpretStep(alias.name, x) }
        }

      interpretations map { is =>
        val defaultPairs: List[(String, Fix[Projected])] =
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
          case CustomPipeline.NotNull(_) =>
            List()
          case x => List(x)
        })

        List(initialProjection) ++ instructions ++ List(lastProjection, CustomPipeline.NotNull(uniqueKey))
      }
    }
  }
}
