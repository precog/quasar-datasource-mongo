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

import matryoshka.birecursiveIso
import matryoshka.data.Fix
import quasar.physical.mongo.{Expression, Optics, CustomPipeline, MongoPipeline, Pipeline, Projection, Step, Field, Index}, Expression._

object Mask {
  val O = Optics.full(birecursiveIso[Fix[Projected], Projected].reverse.asPrism)
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
      case E.ProjectionStep.Field(str) =>
        val insertTo = tree.obj.get(str) getOrElse emptyTree
        val inserted = insert(E.Projection(tail:_*), insertee, insertTo)
        tree.copy(obj = tree.obj.updated(str, inserted))
      case E.ProjectionStep.Index(ix) =>
        val insertTo = tree.list.lift(ix) getOrElse emptyTree
        val inserted = insert(E.Projection(tail:_*), insertee, insertTo)
        tree.copy(list = tree.list.updated(ix, inserted))
    }
  }

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
  private def typeTreeFilters0(undefined: Projection, proj: Projection, tree: TypeTree): Fix[Projected] = {
    val typeStrings: List[Fix[Projected]] = tree.types.toList flatMap parseTypeStrings map (O.string(_))
    val eqExprs: List[Fix[Projected]] = typeStrings map (x => O.$eq(List(O.$type(O.projection(proj)), x)))
    val listExprs: List[Fix[Projected]] =
      if (tree.types.contains(ColumnType.Array)) List()
      else tree.list.toList.sortBy(_._1) map {
        case (i, child) => typeTreeFilters0(undefined, proj + Projection.index(i), child)
      }
    val objExprs: List[Fix[Projected]] =
      if (tree.types.contains(ColumnType.Object)) List()
      else tree.obj.toList map {
        case (key, child) => typeTreeFilters0(undefined, proj + Projection.key(key), child)
      }
    O.$or(eqExprs ++ listExprs ++ objExprs)
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

  private def rebuildDoc(undefined: Projection, proj: Projection, tree: TypeTree): Fix[Projected] = {

    lazy val obj: Fix[Projected] = {
      val treeMap = tree.obj map {
        case (key, child) => (key, rebuildDoc(undefined, proj + Projection.key(key), child))
      }
      O.obj(treeMap)
    }

    lazy val array: Fix[Projected] = {
      val treeList =
        tree.list.toList.sortBy(_._1) map {
          case (i: Int, child: TypeTree) => rebuildDoc(undefined, proj + Projection.index(i), child)
      }
      O.array(treeList)
    }

    def isArray(e: Fix[Projected]): Fix[Projected] =
      O.$eq(List(O.$type(e), O.string("array")))

    def isObject(e: Fix[Projected]): Fix[Projected] =
      O.$eq(List(O.$type(e), O.string("object")))

    def arrayOr(e: Fix[Projected]): Fix[Projected] =
      if (tree.types.contains(ColumnType.Array) || tree.list.isEmpty) e
      else O.$cond(isArray(O.projection(proj)), array, e)

    def objectOr(e: Fix[Projected]): Fix[Projected] =
      if (tree.types.contains(ColumnType.Object) || tree.obj.isEmpty) e
      else O.$cond(isObject(O.projection(proj)), obj, e)

    def projectionOr(e: Fix[Projected]): Fix[Projected] =
      if (tree.types.isEmpty) e else O.projection(proj)

    // we need double check to exclude empty objects and other redundant stuff
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

  def apply0(uniqueKey: String, masks: Map[CPath, Set[ColumnType]]): Option[List[Pipeline[Fix[Projected]]]] = {
    val undefinedKey = uniqueKey.concat("_non_existent_field")
    if (masks.isEmpty) Some(List(Pipeline.$match(Map(undefinedKey -> O.bool(true)))))
    else masksToTypeTree0(uniqueKey, masks) map { tree =>
      val projected = rebuildDoc(Projection.key(undefinedKey), Projection(List()), tree)
      val projectMap = Map(uniqueKey -> projected)
      List(Pipeline.$project(projectMap), CustomPipeline.NotNull(uniqueKey))
    }
  }
}
