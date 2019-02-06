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

import cats.syntax.order._
import cats.syntax.traverse._
import cats.syntax.foldable._
import cats.syntax.flatMap._
import cats.instances.option._
import cats.instances.list._
import quasar.common.{CPath, CPathField, CPathIndex, CPathNode}

import quasar.ParseInstruction

import org.bson.BsonValue

import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}, Aggregator._

import shims._

object Wrap {
  def apply(
      uniqueKey: String,
      version: Version,
      processed: List[ParseInstruction],
      path: CPath,
      name: String)
      : Option[List[Aggregator]] = {

    if (version < Version(3, 4, 0)) None
    else {
      final case class AggPrepare(currentPath: E.Projection, pathsToUnwind: List[E.Projection], condition: MongoExpression)

      val initialPreparation: AggPrepare = AggPrepare(E.Projection(), List(), E.Bool(true))

      val preparations: Option[AggPrepare] =
        path.nodes.foldM(initialPreparation) { (prep: AggPrepare, node: CPathNode) => node match {
          case CPathField(str) => Some(prep.copy(currentPath = prep.currentPath +/ E.key(str)))
          case CPathIndex(i) =>
            val idX: E.Projection = E.key("id" ++ prep.pathsToUnwind.length.toString)
            Some(prep.copy(pathsToUnwind = prep.currentPath +: prep.pathsToUnwind, condition =
              E.Object("$and" -> E.Array(prep.condition, E.Object("$eq" -> E.Array(idX, E.Int(i)))))))
          case _ => None
        }}


      preparations map { ps =>
        val indexed: List[(E.Projection, Int)] = ps.pathsToUnwind.zipWithIndex

        val unwinds = indexed map { case (p: E.Projection, i: Int) => Unwind(p, "id" ++ i.toString) }

        val groupsAndFlatten: List[Aggregator] = indexed.reverse foldMap { case (p: E.Projection, i: Int) => {
          val groupId = E.Array({
            List.range(0, i) map (ix => E.key("id" ++ ix.toString)) }: _*)

          val groupObj =
            E.Object(
              uniqueKey -> E.Object("$$first" -> E.key(uniqueKey)),
              "acc" -> E.Object("$$push" -> p))

          val group = Aggregator.group(groupId, groupObj)

          val moveAcc = Aggregator.addFields(E.Object(
            (E.key(uniqueKey) +/ p).toKey -> E.key("acc")))

          val projectRoot = Aggregator.project(E.Object(
            uniqueKey -> E.Bool(true)))

          List(group, moveAcc, projectRoot)
        }}

        def cond(test: MongoExpression, ok: MongoExpression, fail: MongoExpression) =
          E.Object("$$cond" -> E.Array(test, ok, fail))

        val wrap = Aggregator.addFields(E.Object(
          ps.currentPath.toString -> cond(
            ps.condition,
            E.Object(name -> ps.currentPath),
            ps.currentPath)))

        unwinds ++ List(wrap) ++ groupsAndFlatten
      }
    }
  }

}
