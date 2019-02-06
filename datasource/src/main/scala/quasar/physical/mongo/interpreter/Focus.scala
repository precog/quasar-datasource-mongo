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

import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Aggregator, MongoExpression => E}

final case class Focus(prefix: E.Projection, index: Int, suffix: List[E.Field])

object Focus {
  def focuses(projection: E.Projection): (List[E.Field], List[Focus]) = {
    @scala.annotation.tailrec
    def impl(
        fields: List[E.Field],
        focuses: List[Focus],
        steps: List[E.ProjectionStep])
        : (List[E.Field], List[Focus]) = steps match {

      case List() => (fields.reverse, focuses.reverse)
      case hd :: tail => hd match {
        case fld @ E.Field(_) => impl(fld :: fields, focuses, tail)
        case E.Index(ix) =>
          val newFocus = Focus(E.Projection(tail.reverse:_*), ix, fields)
          impl(List(), newFocus :: focuses, tail)
      }
    }
    impl(List(), List(), projection.steps.toList.reverse)
  }

  val ToInsertField: String = "to_insert"
  val TargetField: String = "target"

  def focusAggregators(focus: Focus): List[Aggregator] = {
    val setTarget =
      Aggregator.addFields(E.Object(TargetField -> (focus.prefix +/ E.index(focus.index))))
    val insertTo =
      E.key(TargetField) +/ E.Projection(focus.suffix:_*)
    val insert =
      Aggregator.addFields(E.Object(insertTo.toKey -> E.key(ToInsertField)))
    val step =
      Aggregator.addFields(E.Object(ToInsertField -> E.helpers.onIndex(focus.prefix, focus.index, (x => E.key(TargetField)))))

    List(setTarget, insert, step)
  }

  def setByFocuses(value: MongoExpression, topFields: List[E.Field], focuses: List[Focus]): List[Aggregator] =
    if (focuses.isEmpty) {
      val proj = E.Projection(topFields:_*)
      List(Aggregator.addFields(E.Object(proj.toKey -> value)))
    } else {
      val initialInsert =
        Aggregator.addFields(E.Object(ToInsertField -> value))
      val proj =
        E.Projection(topFields:_*)
      val lastInsert =
        Aggregator.addFields(E.Object(proj.toKey -> E.key(ToInsertField)))
      val cleanUp =
        Aggregator.project(E.Object(ToInsertField -> E.Int(0), TargetField -> E.Int(0)))

      List(initialInsert) ++ (focuses flatMap focusAggregators) ++ List(lastInsert, cleanUp)
    }
}
