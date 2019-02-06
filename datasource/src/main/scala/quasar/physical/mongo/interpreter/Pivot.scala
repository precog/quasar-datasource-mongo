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
import cats.syntax.foldable._
import cats.instances.option._
import cats.instances.list._
import cats.instances.map._

import quasar.{ParseInstruction, CompositeParseType, IdStatus, ParseType}
import quasar.common.{CPath, CPathField, CPathIndex, CPathNode}

import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}, Aggregator._

import shims._

object Pivot {
  // TODO: this is common stuff
  final case class AggPrepare(currentPath: E.Projection, pathsToUnwind: List[(E.Projection, Int)])

  val initialPreparation: AggPrepare = AggPrepare(E.Projection(), List())


  def prepareCPath(path: CPath): Option[AggPrepare] =
    path.nodes.foldM(initialPreparation) { (prep: AggPrepare, node: CPathNode) => node match {
      case CPathField(str) => Some(prep.copy(currentPath = prep.currentPath +/ E.key(str)))
      case CPathIndex(i) => Some(prep.copy(pathsToUnwind = (prep.currentPath, i) +: prep.pathsToUnwind))
      case _ => None
    }}

  def mkUnwindAndMatch(projection: E.Projection, ix: Int, key: String): List[Aggregator] = {
    val unwind = Aggregator.unwind(projection, key)
    val mmatch = Aggregator.mmatch(E.Object(key -> E.Int(ix)))
    List(unwind, mmatch)
  }

  def mkGroupBys(root: String, groupping: E.Projection, keys: List[String]): List[Aggregator] = {
    val groupId = E.Array({ keys map (E.key(_)) }: _*)

    val groupObj = E.Object(
      root -> E.Object("$$first" -> E.key(root)),
      "acc" -> E.Object("$$push" -> groupping))

    val group = Aggregator.group(groupId, groupObj)

    val moveAcc = Aggregator.addFields(E.Object(
      groupping.toString -> E.key("acc")
    ))

    val projectRoot = Aggregator.project(E.Object(
     root -> E.Bool(true)))

    List(group, moveAcc, projectRoot)
  }

  def preparationBrackets(root: String, preparation: AggPrepare): (List[Aggregator], List[Aggregator]) = {
    def mkKey(i: Int): String = root ++ i.toString

    val pathIdPairs = preparation.pathsToUnwind.zipWithIndex

    val before = pathIdPairs flatMap {
      case ((p: E.Projection, ix: Int), unwindIx: Int) => mkUnwindAndMatch(p, ix, mkKey(unwindIx))
    }
    val after = pathIdPairs flatMap {
      case ((p: E.Projection, ix: Int), unwindIx: Int) =>
        val keys = "_id" +: (List.range(0, unwindIx) map mkKey)
        mkGroupBys(root, p, keys)
    }
    (before, after)
  }

  def matchStructure(path: E.Projection, structure: CompositeParseType): Aggregator =
    Aggregator.mmatch(E.Object(
      path.toString -> E.Object(
        "$$type" -> E.String(structure match {
          case ParseType.Array => "array"
          case ParseType.Object => "object"
          case ParseType.Meta => "omit"
        }))
    ))

  def pivotArray(key: String, path: E.Projection, status: IdStatus): List[Aggregator] = {
    val unwind = Aggregator.unwind(path, key)
    val project = Aggregator.project(E.Object(
      path.toString -> (status match {
        case IdStatus.IdOnly => E.String("$$" ++ key)
        case IdStatus.ExcludeId => E.String("$$" ++ path.toString)
        case IdStatus.IncludeId => E.Array(
          E.String("$$" ++ key),
          E.String("$$" ++ path.toString))
      })))
    List(unwind, project)
  }

  def pivotObject(key: String, path: E.Projection, status: IdStatus): List[Aggregator] = {
    val toArray = Aggregator.project(E.Object(
      path.toString -> E.Object(
        "objectToArray" -> E.String("$$".concat(path.toString)))))

    val unwind = Aggregator.unwind(path, key)

    val project = Aggregator.project(E.Object(
      path.toString -> (status match {
        case IdStatus.IdOnly => E.String("$$" ++ path.toString ++ ".k")
        case IdStatus.ExcludeId => E.String("$$" ++ path.toString ++ ".v")
        case IdStatus.IncludeId => E.Array(
          E.String("$$" ++ path.toString ++ ".k"),
          E.String("$$" ++ path.toString ++ ".v"))
      })))
    List(toArray, unwind, project)
  }

  def apply(
      uniqueKey: String,
      version: Version,
      path: CPath,
      status: IdStatus,
      structure: CompositeParseType)
      : Option[List[Aggregator]] = {

    if (version < Version(3, 4, 0)) None
    else for {
      prepared <- prepareCPath(path)
      idKey = uniqueKey ++ "_pivotId"
      pivot <- structure match {
        case ParseType.Array => Some(pivotArray(idKey, prepared.currentPath, status))
        case ParseType.Object => Some(pivotArray(idKey, prepared.currentPath, status))
        case _ => None
      }
      brackets = preparationBrackets(uniqueKey, prepared)
    } yield brackets._1 ++ pivot ++ brackets._2
  }
}
