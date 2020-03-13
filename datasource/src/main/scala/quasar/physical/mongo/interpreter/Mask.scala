/*
 * Copyright 2020 Precog Data
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
import quasar.api.ColumnType

import quasar.physical.mongo.expression._
import quasar.physical.mongo.utils.optToAlternative

import scalaz.{MonadState, Scalaz, PlusEmpty, Reader, MonadReader}, Scalaz._

import shims.orderToScalaz

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
    case ColumnType.String => List("string", "objectId")
    case ColumnType.OffsetDateTime => List("date")
    case ColumnType.Array => List("array")
    case ColumnType.Object => List("object")
    case _ => List()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def typeTreeFilters(proj: Projection, tree: TypeTree): Expr = {
    if (tree.types === ColumnType.Top) {
      O.$not(O.$eq(List(
        O.$type(O.projection(proj)),
        O.string("missing"))))
    } else {
      val typeExprs: List[Expr] = tree.types.toList flatMap parseTypeStrings map (O.string(_))
      val eqExprs: List[Expr] = typeExprs map (x => O.$eq(List(O.$type(O.projection(proj)), x)))
      val listExprs: List[Expr] =
        if (tree.types.contains(ColumnType.Array)) List()
        else tree.list.toList.sortBy(_._1) map {
          case (i, child) => typeTreeFilters(proj + Projection.index(i), child)
        }
      val objExprs: List[Expr] =
        if (tree.types.contains(ColumnType.Object)) List()
        else tree.obj.toList map {
          case (key, child) => typeTreeFilters(proj + Projection.key(key), child)
        }
      O.$or(eqExprs ++ listExprs ++ objExprs)
    }

  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def rebuildDoc[F[_]: MonadReader[?[_], Config]](
      proj: Projection,
      tree: TypeTree)
      : F[Expr] = MonadReader[F, Config].ask flatMap { case Config(uniqueKey, undefined, mapper) =>


    val obj: F[Expr] = {
      val treeMap = tree.obj.toList.foldLeftM(Map[String, Expr]()) {
        case (m, (key, child)) =>
          rebuildDoc(proj + Projection.key(key), child) map (m.updated(key, _))
      }
      treeMap map (O.obj(_))
    }

    val array: F[Expr] = inArray {
      for {
        arr <- tree.list.toList.sortBy(_._1) traverse {
          case (i, child) => rebuildDoc(proj + Projection.index(i), child)
        }
        undef <- MonadReader[F, Config].asks(_.undefined)
      } yield
        O.$reduce(
          O.array(arr),
          O.array(List()),
          O.$cond(
            O.$eq(List(O.string("$$this"), undef)),
            O.string("$$value"),
            O.$concatArrays(List(
              O.string("$$value"),
              O.array(List(O.string("$$this")))))))
    }

    def isArray(e: Expr): Expr =
      O.$eq(List(O.$type(e), O.string("array")))

    def isObject(e: Expr): Expr =
      O.$eq(List(O.$type(e), O.string("object")))

    def arrayOr(e: Expr): F[Expr] =
      if (tree.types.contains(ColumnType.Array) || tree.list.isEmpty) e.point[F]
      else array map (O.$cond(isArray(O.projection(proj)), _, e))

    def objectOr(e: Expr): F[Expr] =
      if (tree.types.contains(ColumnType.Object) || tree.obj.isEmpty) e.point[F]
      else inObject(obj) map (O.$cond(isObject(O.projection(proj)), _, e))

    def projectionOr(e: Expr): Expr =
      if (tree.types.isEmpty) e else O.projection(proj)

    def rootWrapper[A](x: F[A]): F[A] =
      if (proj === Projection.key(uniqueKey)) mapper match {
        case Mapper.Focus(key) if key === uniqueKey => inArray(x)
        case _ => x
      } else x

    if (proj === Projection(List())) {
      if (tree.obj.isEmpty) {
        if (tree.types.contains(ColumnType.Object)) O.string("$$ROOT").point[F]
        else undefined.point[F]
      } else if (tree.obj.keySet === Set(uniqueKey) && mapper === Mapper.Focus(uniqueKey)) {
        inArray(obj)
      } else inObject(obj)
    }
    else rootWrapper(for {
      obj <- objectOr(projectionOr(undefined))
      arr <- arrayOr(obj)
    } yield O.$cond(typeTreeFilters(proj, tree), arr, undefined))
  }

  final case class Config(uniqueKey: String, undefined: Expr, mapper: Mapper)

  def inArray[F[_]: MonadReader[?[_], Config], A](action: F[A]): F[A] = {
    val modify: Config => Config = cfg => cfg.copy(undefined = missing(cfg.uniqueKey))
    MonadReader[F, Config].local(modify)(action)
  }
  def inObject[F[_]: MonadReader[?[_], Config], A](action: F[A]): F[A] = {
    val modify: Config => Config = cfg => cfg.copy(undefined = missingKey(cfg.uniqueKey))
    MonadReader[F, Config].local(modify)(action)
  }

  def apply[F[_]: MonadInState: PlusEmpty](masks: Map[CPath, Set[ColumnType]]): F[List[Pipe]] =
    MonadState[F, InterpretationState].get flatMap { state =>
      val eraseId = Map("_id" -> O.int(0))

      def projectionObject(tree: TypeTree): Option[Map[String, Expr]] = {
        val initialConfig = Config(state.uniqueKey, missing(state.uniqueKey), state.mapper)
        val rebuilt = rebuildDoc[Reader[Config, ?]](Projection(List()), tree).run(initialConfig)
        state.mapper match {
          case Mapper.Unfocus => Some(Map(state.uniqueKey -> rebuilt))
          case Mapper.Focus(_) => O.obj.getOption(rebuilt)
        }
      }

      if (masks.isEmpty) {
        List(Pipeline.Erase: Pipe).point[F]
      }
      else for {
        projObj <- optToAlternative[F].apply(for {
          tree <- masksToTypeTree(state.mapper, masks)
          pObj <- projectionObject(tree)
        } yield pObj)
        _ <- focus[F]
      } yield List(Pipeline.$project(eraseId ++ projObj), Pipeline.Presented)
    }
}
