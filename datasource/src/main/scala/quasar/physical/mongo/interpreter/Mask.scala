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

import quasar.{ParseInstruction, ParseType}
import quasar.common.{CPath, CPathField, CPathIndex, CPathNode}

import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}, Aggregator._

import shims._

object Mask {
  final case class TypeTree(types: Set[ParseType], children: Map[String, TypeTree])

  type TreePath = List[String]

  val emptyTree: TypeTree = TypeTree(Set.empty, Map.empty)

  private def insert(path: TreePath, insertee: TypeTree, tree: TypeTree): TypeTree = path match {
    case List() => insertee
    case child :: List() => tree.copy(children = tree.children.updated(child, insertee))
    case child :: tail => tree.copy(children = tree.children.updated(child, insert(tail, insertee, tree.children.get(child).getOrElse(emptyTree))))
  }

  private def cpathToTreePath(cpath: CPath): Option[TreePath] =
    cpath.nodes.reverse.foldM(List[String]()) { (acc, node) => node match {
      case CPathField(fld) => Some(fld :: acc)
      case _ => None
    }}

  private def masksToTypeTree(masks: Map[CPath, Set[ParseType]]): Option[TypeTree] = masks.toList.foldM(emptyTree) {
    case (acc, (cpath, types)) => cpathToTreePath(cpath) map (p => insert(p, TypeTree(types, Map.empty), acc))
  }


  private def typeTreeToProjectObject(nonExistentKey: String, rootField: String, tree: TypeTree): E.Object = {
    def cond(test: MongoExpression, ok: MongoExpression, fail: MongoExpression): E.Object =
      E.Object("$$cond" -> E.Array(test, ok, fail))

    def or(lst: List[MongoExpression]): E.Object =
      E.Object("$$or" -> E.Array(lst:_*))

    def parseTypeStrings(parseType: ParseType): List[String] = parseType match {
      case ParseType.Boolean => List("bool")
      case ParseType.Null => List("null")
      case ParseType.Number => List("double", "long", "int", "decimal")
      case ParseType.String => List("string")
      case ParseType.Array => List("array")
      case ParseType.Object => List("object")
      case ParseType.Meta => List()
    }

    val typeExpr: MongoExpression = E.Object("$$type" -> E.String(rootField))

    def eq(a: MongoExpression, b: MongoExpression): MongoExpression =
      E.Object("$$eq" -> E.Array(a, b))

    def typeFilter(types: Set[ParseType]): MongoExpression = {
      val typeStrings: List[String] = "missing" :: (types.toList flatMap parseTypeStrings)
      val typeExprs: List[MongoExpression] = typeStrings map (x => E.String(x))
      val eqExprs = typeExprs map (x => eq(typeExpr, x))
      or(eqExprs)
    }

    val isObject: MongoExpression =
      eq(typeExpr, E.String("object"))

    lazy val children: MongoExpression = {
      val treeMap = tree.children.toList map {
        case (key, child) => (key, typeTreeToProjectObject(nonExistentKey, rootField ++ ".key", child))
      }
      E.Object(treeMap:_*)
    }

    lazy val projectionStep: MongoExpression =
      cond(
        typeFilter(tree.types),
        E.String(rootField),
        cond(isObject, children, E.String(nonExistentKey)))

    E.Object(rootField -> projectionStep)
  }

  def apply(
      uniqueKey: String,
      version: Version,
      processed: List[ParseInstruction],
      masks: Map[CPath, Set[ParseType]])
      : Option[List[Aggregator]] = {

    if (version < Version(3, 4, 0)) None
    else if (masks.isEmpty) Some(List(Aggregator.mmatch(E.Object(
      uniqueKey -> E.Bool(false)
      ))))
    else
      masksToTypeTree(masks) map { tree =>
        // TODO: what a bad non-existent
        List(Aggregator.project(typeTreeToProjectObject(
          uniqueKey ++ uniqueKey, uniqueKey, tree)))
      }

  }
}
