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

import quasar.physical.mongo.{Aggregator, Version, MongoExpression, MongoProjection}, Aggregator._

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
      final case class AggPrepare(currentPath: MongoProjection, pathsToUnwind: List[MongoProjection], condition: MongoExpression)

      val initialPreparation: AggPrepare = AggPrepare(MongoProjection.Root, List(), MongoExpression.MongoBoolean(true))

      val preparations: Option[AggPrepare] = path.nodes.foldM(initialPreparation) { (prep: AggPrepare, node: CPathNode) => node match {
        case CPathField(str) => Some(prep.copy(currentPath = prep.currentPath +/ MongoProjection.Field(str)))
        case CPathIndex(i) =>
          val idX: MongoProjection.Field = MongoProjection.Field("id" ++ prep.pathsToUnwind.length.toString).toVar
          Some(prep.copy(pathsToUnwind = prep.currentPath +: prep.pathsToUnwind, condition =
          MongoExpression.MongoObject(Map("$and" -> MongoExpression.MongoArray(List(
            prep.condition, MongoExpression.MongoObject(Map("$eq" -> MongoExpression.MongoArray(List(idX, MongoExpression.MongoInt(i)))))))))))
        case _ => None
      }}


      preparations map { ps =>
        val indexed: List[(MongoProjection, Int)] = ps.pathsToUnwind.zipWithIndex

        val unwinds = indexed map { case (p: MongoProjection, i: Int) => Unwind(p, "id" ++ i.toString) }

        val groupsAndFlatten: List[Aggregator] = indexed.reverse foldMap { case (p: MongoProjection, i: Int) => {
          val groupId = MongoExpression.MongoArray {
            List.range(0, i) map (ix => MongoProjection.Field("id" ++ ix.toString).toVar) }

          val groupObj = MongoExpression.MongoObject(Map(
            uniqueKey -> MongoExpression.MongoObject(Map("$$first" -> MongoProjection.Field(uniqueKey).toVar)),
            "acc" -> MongoExpression.MongoObject(Map("$$push" -> p.toVar))))

          val group = Aggregator.group(groupId, groupObj)

          val moveAcc = Aggregator.addFields(MongoExpression.MongoObject(Map(
            (MongoProjection.Field(uniqueKey) +/ p).toString -> MongoProjection.Field("acc").toVar)))

          val projectRoot = Aggregator.project(MongoExpression.MongoObject(Map(
            uniqueKey -> MongoExpression.MongoBoolean(true))))

          List(group, moveAcc, projectRoot)
        }}

        def cond(test: MongoExpression, ok: MongoExpression, fail: MongoExpression) = MongoExpression.MongoObject(Map(
          "$$cond" -> MongoExpression.MongoArray(List(test, ok, fail))))

        val wrap = Aggregator.addFields(MongoExpression.MongoObject(Map(
          ps.currentPath.toString -> cond(
            ps.condition,
            MongoExpression.MongoObject(Map(name -> ps.currentPath.toVar)),
            ps.currentPath.toVar))))

        unwinds ++ List(wrap) ++ groupsAndFlatten
      }
    }
  }

}
