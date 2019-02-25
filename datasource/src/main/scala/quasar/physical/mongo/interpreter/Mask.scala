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

import quasar.physical.mongo.expression._

import shims._

object Mask {
  final case class TypeTree(types: Set[ColumnType], obj: Map[String, TypeTree], list: Map[Int, TypeTree])

  val emptyTree: TypeTree = TypeTree(Set.empty, Map.empty, Map.empty)

  def isEmpty(tree: TypeTree): Boolean =
    tree.types.isEmpty && tree.obj.isEmpty && tree.list.isEmpty

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def insert0(steps: List[Step], insertee: TypeTree, tree: TypeTree): TypeTree = steps match {
    case List() =>
      TypeTree(insertee.types ++ tree.types, tree.obj ++ insertee.obj, tree.list ++ insertee.list)
    case child :: tail => child match {
      case Field(str) =>
        val insertTo = tree.obj.get(str) getOrElse emptyTree
        val inserted = insert0(tail, insertee, insertTo)
        tree.copy(obj = tree.obj.updated(str, inserted))
      case Index(ix) =>
        val insertTo = tree.list.lift(ix) getOrElse emptyTree
        val inserted = insert0(tail, insertee, insertTo)
        tree.copy(list = tree.list.updated(ix, inserted))
    }
  }

  private def masksToTypeTree0(uniqueKey: String, masks: Map[CPath, Set[ColumnType]]): Option[TypeTree] = {
    masks.toList.foldM(emptyTree) {
      case (acc, (cpath, types)) => Projection.fromCPath(cpath) map { p =>
        insert0(Field(uniqueKey) :: p.steps, TypeTree(types, Map.empty, Map.empty), acc)
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
  private def typeTreeFilters0(undefined: Projection, proj: Projection, tree: TypeTree): Expr = {
    val typeStrings: List[Expr] = tree.types.toList flatMap parseTypeStrings map (O.string(_))
    val eqExprs: List[Expr] = typeStrings map (x => O.$eq(List(O.$type(O.projection(proj)), x)))
    val listExprs: List[Expr] =
      if (tree.types.contains(ColumnType.Array)) List()
      else tree.list.toList.sortBy(_._1) map {
        case (i, child) => typeTreeFilters0(undefined, proj + Projection.index(i), child)
      }
    val objExprs: List[Expr] =
      if (tree.types.contains(ColumnType.Object)) List()
      else tree.obj.toList map {
        case (key, child) => typeTreeFilters0(undefined, proj + Projection.key(key), child)
      }
    O.$or(eqExprs ++ listExprs ++ objExprs)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def rebuildDoc(undefined: Projection, proj: Projection, tree: TypeTree): Expr = {

    lazy val obj: Expr = {
      val treeMap = tree.obj map {
        case (key, child) => (key, rebuildDoc(undefined, proj + Projection.key(key), child))
      }
      O.obj(treeMap)
    }

    lazy val array: Expr = {
      val treeList =
        tree.list.toList.sortBy(_._1) map {
          case (i: Int, child: TypeTree) => rebuildDoc(undefined, proj + Projection.index(i), child)
      }
      O.array(treeList)
    }

    def isArray(e: Expr): Expr =
      O.$eq(List(O.$type(e), O.string("array")))

    def isObject(e: Expr): Expr =
      O.$eq(List(O.$type(e), O.string("object")))

    def arrayOr(e: Expr): Expr =
      if (tree.types.contains(ColumnType.Array) || tree.list.isEmpty) e
      else O.$cond(isArray(O.projection(proj)), array, e)

    def objectOr(e: Expr): Expr =
      if (tree.types.contains(ColumnType.Object) || tree.obj.isEmpty) e
      else O.$cond(isObject(O.projection(proj)), obj, e)

    def projectionOr(e: Expr): Expr =
      if (tree.types.isEmpty) e else O.projection(proj)

    if (proj === Projection(List())) obj
    else // we need double check to exclude empty objects and other redundant stuff
      O.$cond(
        // At first we filter that there is anything in this or children
        typeTreeFilters0(undefined, proj, tree),
        // and if so, we look if it's nested array
        arrayOr(
          // or nested object
          objectOr(
            // or at least something
            projectionOr(O.projection(undefined)))),
        O.projection(undefined))
  }

  def apply0(uniqueKey: String, masks: Map[CPath, Set[ColumnType]]): Option[List[Pipe]] = {
    val undefinedKey = uniqueKey.concat("_non_existent_field")
    if (masks.isEmpty) Some(List(Pipeline.$match(Map(undefinedKey -> O.bool(true)))))
    else for {
      tree <- masksToTypeTree0(uniqueKey, masks)
      projected = rebuildDoc(Projection.key(undefinedKey), Projection(List()), tree)
      projObj <- O.obj.getOption(projected)
    } yield List(Pipeline.$project(projObj), Pipeline.NotNull(uniqueKey))
  }
}
