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

import cats.syntax.traverse._
import cats.instances.option._
import cats.instances.list._

import quasar.common.CPathField
import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Interpretation, Interpreter, Aggregator, Version, MongoExpression => E}
import quasar.ScalarStage

import shims._

object Cartesian {
  def apply(
      uniqueKey: String,
      version: Version,
      cartouches: Map[CPathField, (CPathField, List[ScalarStage.Focused])],
      interpreter: Interpreter)
      : Option[List[Aggregator]] = {

    if (cartouches.isEmpty) Some(List(Aggregator.filter(E.Object(uniqueKey -> E.Bool(false)))))
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

}
