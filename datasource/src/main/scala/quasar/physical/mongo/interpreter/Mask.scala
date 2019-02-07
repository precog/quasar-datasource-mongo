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

import quasar.ParseType
import quasar.common.CPath

import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}

import shims._

object Mask {
  final case class TypeTree(types: Set[ParseType], obj: Map[String, TypeTree], list: List[TypeTree])

  val emptyTree: TypeTree = TypeTree(Set.empty, Map.empty, List())

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def insert(
      path: E.Projection,
      insertee: TypeTree,
      tree: TypeTree)
      : TypeTree =

  path.steps.toList match {
    case List() => insertee
    case child :: List() => child match {
      case E.Field(str) =>
        tree.copy(obj = tree.obj.updated(str, insertee))
      case E.Index(ix) =>
        val emptyLength: Int = tree.list.length - ix + 1
        val toInsert: List[TypeTree] =
          tree.list ++ (if (emptyLength > 0) List.fill[TypeTree](emptyLength)(emptyTree) else List())
        tree.copy(list = toInsert.updated(ix, insertee))
    }
    case child :: tail => child match {
      case E.Field(str) =>
        val insertTo = tree.obj.get(str) getOrElse emptyTree
        val inserted = insert(E.Projection(tail:_*), insertee, insertTo)
        tree.copy(obj = tree.obj.updated(str, inserted))
      case E.Index(ix) =>
        val insertTo = tree.list.lift(ix) getOrElse emptyTree
        val inserted = insert(E.Projection(tail:_*), insertee, insertTo)
        tree.copy(list = tree.list.updated(ix, inserted))
    }
  }


  private def masksToTypeTree(
      uniqueKey: E.Projection,
      masks: Map[CPath, Set[ParseType]])
      : Option[TypeTree] =

  masks.toList.foldM(emptyTree) {
    case (acc, (cpath, types)) => E.cpathToProjection(cpath) map { p =>
      insert(uniqueKey +/ p, TypeTree(types, Map.empty, List()), acc)
    }
  }


  private def parseTypeStrings(parseType: ParseType): List[String] = parseType match {
    case ParseType.Boolean => List("bool")
    case ParseType.Null => List("null")
    case ParseType.Number => List("double", "long", "int", "decimal")
    case ParseType.String => List("string")
    case ParseType.Array => List("array")
    case ParseType.Object => List("object")
    case ParseType.Meta => List()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def typeTreeToProjectObject(undefined: E.Projection, proj: E.Projection, tree: TypeTree): E.Object = {
    import E.helpers._

    def typeFilter(types: Set[ParseType]): MongoExpression = {
      val typeStrings: List[String] = "missing" :: (types.toList flatMap parseTypeStrings)
      val typeExprs: List[MongoExpression] = typeStrings map (x => E.String(x))
      val eqExprs = typeExprs map (x => equal(typeExpr(proj), x))
      or(eqExprs)
    }

    lazy val obj: MongoExpression = {
      val treeMap = tree.obj.toList map {
        case (key, child) => (key, typeTreeToProjectObject(undefined, proj +/ E.key(key), child))
      }
      E.Object(treeMap:_*)
    }

    lazy val array: MongoExpression = {
      val treeList = tree.list.zipWithIndex map {
        case (child, i) => typeTreeToProjectObject(undefined, proj +/ E.index(i), child)
      }
      E.Array(treeList:_*)
    }

    lazy val projectionStep: MongoExpression =
      cond(typeFilter(tree.types),
        proj,
        cond(isObject(proj),
          obj,
          cond(isArray(proj),
            array,
            undefined)))

    E.Object(proj.toKey -> (if (tree.types.isEmpty) undefined else projectionStep))
  }

  def apply(
      uniqueKey: String,
      version: Version,
      masks: Map[CPath, Set[ParseType]])
      : Option[List[Aggregator]] = {

    if (version < Version(3, 4, 0)) None
    else if (masks.isEmpty) Some(List(Aggregator.filter(E.Object(uniqueKey -> E.Bool(false)))))
    else masksToTypeTree(E.key(uniqueKey), masks) map { tree =>
      List(Aggregator.project(typeTreeToProjectObject(
        E.key(uniqueKey ++ uniqueKey),
        E.key(uniqueKey),
        tree)))
    }
  }
}
