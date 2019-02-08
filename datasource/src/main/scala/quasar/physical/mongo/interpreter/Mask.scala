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
  final case class TypeTree(types: Set[ColumnType], obj: Map[String, TypeTree], list: List[TypeTree])

  val emptyTree: TypeTree = TypeTree(Set.empty, Map.empty, List())

  def isEmpty(tree: TypeTree): Boolean =
    tree.types.isEmpty && tree.obj.isEmpty && tree.list.isEmpty

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def insert(
      path: E.Projection,
      insertee: TypeTree,
      tree: TypeTree)
      : TypeTree =

  path.steps.toList match {
    case List() => TypeTree(insertee.types ++ tree.types, tree.obj ++ insertee.obj, tree.list ++ insertee.list)
    case child :: List() => child match {
      case E.Field(str) =>
        tree.copy(obj = tree.obj.updated(str, insertee))
      case E.Index(ix) =>
        val emptyLength: Int = ix - tree.list.length + 1
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
        val emptyLength: Int = ix - tree.list.length + 1
        val toInsert: List[TypeTree] =
          tree.list ++ (if (emptyLength > 0) List.fill[TypeTree](emptyLength)(emptyTree) else List())
        tree.copy(list = toInsert.updated(ix, inserted))
    }
  }

  private def masksToTypeTree(
      uniqueKey: E.Projection,
      masks: Map[CPath, Set[ColumnType]])
      : Option[TypeTree] = {

    masks.toList.foldM(emptyTree) {
      case (acc, (cpath, types)) => E.cpathToProjection(cpath) map { p =>
        insert(uniqueKey +/ p, TypeTree(types, Map.empty, List()), acc)
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
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def typeTreeToProjectObject(undefined: E.Projection, proj: E.Projection, tree: TypeTree): MongoExpression = {
    import E.helpers._

    def typeFilter(tree: TypeTree): MongoExpression = {
      val typeStrings: List[String] = tree.types.toList flatMap parseTypeStrings
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
      if (tree.list.isEmpty) E.key("non-existent-field")
      else {
        val treeList = tree.list.zipWithIndex map {
          case (child, i) => typeTreeToProjectObject(undefined, proj +/ E.index(i), child)
        }
        E.Object("$reduce" -> E.Object(
          "input" -> E.Array(treeList:_*),
          "initialValue" -> E.Array(),
          "in" -> cond(
            equal(typeExpr(E.String("$$this")), E.String("null")),
            E.String("$$value"),
            E.Object("$concatArrays" -> E.Array(E.String("$$value"), E.Array(E.String("$$this")))))))
      }
    }

    def arrayOr(expr: MongoExpression): MongoExpression =
      if (tree.types.contains(ColumnType.Array) || tree.list.isEmpty) expr
      else cond(isArray(proj), array, expr)

    def objectOr(expr: MongoExpression): MongoExpression =
      if (tree.types.contains(ColumnType.Object) || tree.obj.isEmpty) expr
      else cond(isObject(proj), obj, expr)

    def scalarOr(expr: MongoExpression): MongoExpression =
      if (tree.types.isEmpty) expr
      else cond(typeFilter(tree), proj, expr)

    if (proj === E.Projection()) obj
    else if(isEmpty(tree)) undefined
    else scalarOr(objectOr(arrayOr(undefined)))
  }

  def apply(
      uniqueKey: String,
      version: Version,
      masks: Map[CPath, Set[ColumnType]])
      : Option[List[Aggregator]] = {

    if (version < Version(3, 4, 0)) None
    else if (masks.isEmpty) Some(List(Aggregator.filter(E.Object(uniqueKey -> E.Bool(false)))))
    else masksToTypeTree(E.key(uniqueKey), masks) map { tree =>
      val projectObject =
        typeTreeToProjectObject(
          E.key("non-existent-field"),
          E.Projection(),
          tree)
      scala.Predef.println(s"masks: ${masks}\ntree: ${tree}\nprojectObject: ${projectObject.toBsonValue}\n")
//      val projectTmp = Aggregator.project(E.Object("tmp" -> projectObject))
//      val back = Aggregator.project(E.Object(uniqueKey -> E.key("tmp")))
      val project = Aggregator.project(projectObject)
      val filterNulls = Aggregator.filter(E.Object(uniqueKey -> E.Object("$ne" -> E.Null)))
      List(project, filterNulls)
    }
  }
}
