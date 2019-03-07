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

import quasar.common.CPath
import quasar.api.table.ColumnType

import quasar.physical.mongo.expression._
import quasar.physical.mongo.utils.optToAlternative

import scalaz.{MonadState, Scalaz, PlusEmpty}, Scalaz._

import shims._

object Mask {
  final case class TypeTree(types: Set[ColumnType], obj: Map[String, TypeTree], list: Map[Int, TypeTree])

  val emptyTree: TypeTree = TypeTree(Set.empty, Map.empty, Map.empty)

  def isEmpty(tree: TypeTree): Boolean =
    tree.types.isEmpty && tree.obj.isEmpty && tree.list.isEmpty

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def insert(steps: List[Step], insertee: TypeTree, tree: TypeTree): TypeTree = steps match {
    case List() =>
      TypeTree(insertee.types ++ tree.types, tree.obj ++ insertee.obj, tree.list ++ insertee.list)
    case child :: tail => child match {
      case Field(str) =>
        val insertTo = tree.obj.get(str) getOrElse emptyTree
        val inserted = insert(tail, insertee, insertTo)
        tree.copy(obj = tree.obj.updated(str, inserted))
      case Index(ix) =>
        val insertTo = tree.list.lift(ix) getOrElse emptyTree
        val inserted = insert(tail, insertee, insertTo)
        tree.copy(list = tree.list.updated(ix, inserted))
    }
  }

  private def masksToTypeTree(mapper: Mapper, masks: Map[CPath, Set[ColumnType]]): Option[TypeTree] = {
    masks.toList.foldLeftM(emptyTree) {
      case (acc, (cpath, types)) =>
        Projection.fromCPath(cpath)
          .map(Mapper.projection(mapper))
          .map(p => insert(p.steps, TypeTree(types, Map.empty, Map.empty), acc))
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
  private def typeTreeFilters(undefined: Projection, proj: Projection, tree: TypeTree): Expr = {
    val typeExprs: List[Expr] = tree.types.toList flatMap parseTypeStrings map (O.string(_))
    val eqExprs: List[Expr] = typeExprs map (x => O.$eq(List(O.$type(O.projection(proj)), x)))
    val listExprs: List[Expr] =
      if (tree.types.contains(ColumnType.Array)) List()
      else tree.list.toList.sortBy(_._1) map {
        case (i, child) => typeTreeFilters(undefined, proj + Projection.index(i), child)
      }
    val objExprs: List[Expr] =
      if (tree.types.contains(ColumnType.Object)) List()
      else tree.obj.toList map {
        case (key, child) => typeTreeFilters(undefined, proj + Projection.key(key), child)
      }
    O.$or(eqExprs ++ listExprs ++ objExprs)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def rebuildDoc(uniqueKey: String, undefined: Projection, proj: Projection, tree: TypeTree): Expr = {

    lazy val obj: Expr = {
      val treeMap = tree.obj map {
        case (key, child) => (key, rebuildDoc(uniqueKey, undefined, proj + Projection.key(key), child))
      }
      O.obj(treeMap)
    }

    lazy val array: Expr = {
      val treeList =
        tree.list.toList.sortBy(_._1) map {
          case (i: Int, child: TypeTree) => rebuildDoc(uniqueKey, undefined, proj + Projection.index(i), child)
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

    if (proj === Projection(List())) {
      if (!tree.obj.isEmpty) obj
      else if (!tree.types.contains(ColumnType.Object)) O.projection(undefined)
      else O.obj(Map(uniqueKey -> O.string("$$ROOT")))
    }
    else // we need double check to exclude empty objects and other redundant stuff
      O.$cond(
        // At first we filter that there is anything in this or children
        typeTreeFilters(undefined, proj, tree),
        // and if so, we look if it's nested array
        arrayOr(
          // or nested object
          objectOr(
            // or at least something
            projectionOr(O.projection(undefined)))),
        O.projection(undefined))
  }

  def apply[F[_]: MonadInState: PlusEmpty](masks: Map[CPath, Set[ColumnType]]): F[List[Pipe]] =
    MonadState[F, InterpretationState].get flatMap { state =>
      val undefinedKey = state.uniqueKey concat "_non_existent_field"
      if (masks.isEmpty)
        List(Pipeline.$match(O.obj(Map(undefinedKey -> O.bool(false)))): Pipe).point[F]
      else for {
        projObj <- optToAlternative[F].apply(for {
          tree <- masksToTypeTree(state.mapper, masks)
          rebuilt = rebuildDoc(state.uniqueKey, Projection.key(undefinedKey), Projection(List()), tree)
          pObj <- O.obj.getOption(rebuilt)
        } yield pObj)
        _ <- focus[F]
      } yield List(Pipeline.$project(projObj), Pipeline.NotNull(state.uniqueKey))
    }
}
