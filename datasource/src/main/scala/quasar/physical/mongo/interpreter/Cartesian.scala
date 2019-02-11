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

import quasar.api.table.ColumnType
import quasar.common.CPathField
import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Interpretation, Interpreter, Aggregator, Version, MongoExpression => E}
import quasar.ScalarStage

import shims._

object Cartesian {
  @scala.annotation.tailrec
  private def flattenAggregators(
      defaultObject: E.Object,
      accum: List[Aggregator],
      positioned: List[List[Aggregator]])
      : List[Aggregator] = {

    def spanProjectPrefix(inp: List[Aggregator]): (Option[E.Object], List[Aggregator]) = inp match {
      case Aggregator.project(a @ E.Object(_)) :: tail => (Some(a), tail)
      case hd :: tail => (None, tail)
      case List() => (None, List())
    }

    @scala.annotation.tailrec
    def spanHead(
        head: Option[Aggregator],
        processed: List[List[Aggregator]],
        inp: List[List[Aggregator]])
        : Option[(Aggregator, List[List[Aggregator]])] = inp match {
      case List() =>
        head map (x => (x, processed.reverse))
      case hd :: inpTail => head match {
        case Some(u) => spanHead(head, hd :: processed, inpTail)
        case None => hd match {
          case aggregator :: nestedTail => spanHead(Some(aggregator), nestedTail :: processed, inpTail)
          case other => spanHead(None, other :: processed, inpTail)
        }
      }
    }

    def takeHead(head: Option[Aggregator], nestedList: List[List[Aggregator]]): Option[Aggregator] =
      nestedList.foldLeft(None: Option[Aggregator]) { (acc: Option[Aggregator], lst: List[Aggregator]) =>
        acc orElse lst.headOption }

    val spannedPrefix = positioned map spanProjectPrefix

    val projects = spannedPrefix flatMap (_._1.toList)

    val next = spannedPrefix map (_._2)

    if (projects.isEmpty) {
      spanHead(None, List(), positioned) match {
        case None => accum.reverse
        case Some((agg, next)) =>
          flattenAggregators(defaultObject, agg :: accum, next)
      }
    }
    else {
      val projectionSum = Aggregator.project(projects.foldLeft(defaultObject)(_ + _))
      val next = spannedPrefix map (_._2)
      flattenAggregators(defaultObject, projectionSum :: accum, next)

    }
  }


  def apply(
      uniqueKey: String,
      version: Version,
      cartouches: Map[CPathField, (CPathField, List[ScalarStage.Focused])],
      interpreter: Interpreter)
      : Option[List[Aggregator]] = {

    if (cartouches.isEmpty) Some(List(Aggregator.filter(E.Object(uniqueKey -> E.Bool(false)))))
    else {
      val cartoucheList = cartouches.toList

      val interpretations: Option[List[List[MongoExpression]]] =
        cartoucheList.traverse {
          case (alias, (_, instructions)) =>
            val interpreted: Interpretation =
              interpreter.refineInterpretation(alias.name, Interpretation.initial(instructions))
            if (!interpreted.stages.isEmpty) None
            else Some(interpreted.aggregators)
        }

      val defaultPairs: List[(String, MongoExpression)] =
        cartoucheList map {
          case (alias, _) => (alias.name, E.key(alias.name))
        }


      interpretations map { is =>
        val initialProjectionPairs = cartoucheList map {
          case (alias, (field, instructions)) => alias.name -> (E.key(uniqueKey) +/ E.key(field.name))
        }
        val initialProjection = Aggregator.project(E.Object(initialProjectionPairs:_*))
        val lastProjectionPairs = cartoucheList map {
          case (alias, _) => alias.name -> E.key(alias.name)
        }
        val lastProjection = Aggregator.project(E.Object(uniqueKey -> E.Object(lastProjectionPairs:_*)))
        val flatten = flattenAggregators(E.Object(defaultPairs:_*), List(), is) ++ List(lastProjection)
        scala.Predef.println(s"FLATTEN ::: ${flatten map (_.toDocument)}")
        List(initialProjection) ++ flattenAggregators(E.Object(defaultPairs:_*), List(), is) ++ List(lastProjection)
      }
    }
  }

}
