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

import quasar.common.CPath
import quasar.api.table.ColumnType

import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}

import shims._

object Mask {
  final case class TypeTree(types: Set[ColumnType], obj: Map[String, TypeTree], list: Map[Int, TypeTree])

  val emptyTree: TypeTree = TypeTree(Set.empty, Map.empty, Map.empty)

  def isEmpty(tree: TypeTree): Boolean =
    tree.types.isEmpty && tree.obj.isEmpty && tree.list.isEmpty

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def insert(
      path: E.Projection,
      insertee: TypeTree,
      tree: TypeTree)
      : TypeTree =

  path.steps.toList match {
    case List() =>
      TypeTree(insertee.types ++ tree.types, tree.obj ++ insertee.obj, tree.list ++ insertee.list)
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
      masks: Map[CPath, Set[ColumnType]])
      : Option[TypeTree] = {

    masks.toList.foldM(emptyTree) {
      case (acc, (cpath, types)) => E.cpathToProjection(cpath) map { p =>
        insert(uniqueKey +/ p, TypeTree(types, Map.empty, Map.empty), acc)
      }
    }
  }

  private def parseTypeStrings(parseType: ColumnType): List[String] = parseType match {
    case ColumnType.Boolean => List("bool")
    case ColumnType.Null => List("null")
    case ColumnType.Number => List("double", "long", "int", "decimal")
    case ColumnType.String => List("string")
    case ColumnType.OffsetDateTime => List("date")
    case ColumnType.Array => List("array")
    case ColumnType.Object => List("object")
    case _ => List()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def typeTreeFilters(undefined: E.Projection, proj: E.Projection, tree: TypeTree): MongoExpression = {
    import E.helpers._
    val typeStrings: List[String] = tree.types.toList flatMap parseTypeStrings
    val typeExprs: List[MongoExpression] = typeStrings map E.String
    val eqExprs: List[E.Object] = typeExprs map (x => equal(typeExpr(proj), x))
    val listExprs: List[MongoExpression] =
      if (tree.types.contains(ColumnType.Array)) List()
      else tree.list.toList.sortBy(_._1) map {
        case (i, child) => typeTreeFilters(undefined, proj +/ E.index(i), child)
      }
    val objExprs: List[MongoExpression] =
      if (tree.types.contains(ColumnType.Object)) List()
      else tree.obj.toList map {
        case (key, child) => typeTreeFilters(undefined, proj +/ E.key(key), child)
      }
    or(eqExprs ++ listExprs ++ objExprs)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def typeTreeToProjectObject(
      undefined: E.Projection,
      proj: E.Projection,
      tree: TypeTree)
      : MongoExpression = {
    import E.helpers._

    lazy val obj: MongoExpression = {
      val treeMap = tree.obj.toList map {
        case (key, child) => (key, typeTreeToProjectObject(undefined, proj +/ E.key(key), child))
      }
      E.Object(treeMap:_*)
    }

    lazy val array: MongoExpression = {
      val treeList =
        tree.list.toList.sortBy(_._1) map {
          case (i: Int, child: TypeTree) => typeTreeToProjectObject(undefined, proj +/ E.index(i), child)
      }
      E.Array(treeList:_*)
    }

    def arrayOr(expr: MongoExpression): MongoExpression =
      if (tree.types.contains(ColumnType.Array) || tree.list.isEmpty) expr
      else cond(isArray(proj), array, expr)

    def objectOr(expr: MongoExpression): MongoExpression =
      if (tree.types.contains(ColumnType.Object) || tree.obj.isEmpty) expr
      else cond(isObject(proj), obj, expr)

    def projectionOr(expr: MongoExpression): MongoExpression =
      if (tree.types.isEmpty) expr else proj

    if (proj === E.Projection()) obj
    else
      // we need double check to exclude empty objects and other redundant stuff
      cond(
        // At first we filter that there is anything in this or children
        typeTreeFilters(undefined, proj, tree),
        // and if so, we look if it's nested array
        arrayOr(
          // or nested object
          objectOr(
            // or at least something
            projectionOr(undefined))),
        undefined)
  }

  def apply(
      uniqueKey: String,
      version: Version,
      masks: Map[CPath, Set[ColumnType]])
      : Option[List[Aggregator]] = {

    val undefinedKey = uniqueKey.concat("_non_existent_field")

    if (version < Version.$type) None
    else if (masks.isEmpty) Some(List(Aggregator.filter(E.Object(undefinedKey -> E.Bool(false)))))
    else masksToTypeTree(E.key(uniqueKey), masks) map { tree =>
      val projectObject =
        typeTreeToProjectObject(
          E.key(undefinedKey),
          E.Projection(),
          tree)
      List(Aggregator.project(projectObject), Aggregator.notNull(uniqueKey))
    }
  }
}
