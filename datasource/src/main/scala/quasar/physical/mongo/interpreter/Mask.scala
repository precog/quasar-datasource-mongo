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

import quasar.ScalarStage
import quasar.api.ColumnType
import quasar.common.CPath
import quasar.physical.mongo.Interpreter
import quasar.physical.mongo.expression._

import cats._
import cats.implicits._
import cats.mtl.{ApplicativeAsk, ApplicativeLocal, MonadState}
import cats.mtl.implicits._
import higherkindness.droste.Basis

import Projection._, Step._

object Mask {
  final case class TypeTree(types: Set[ColumnType], obj: Map[String, TypeTree], list: Map[Int, TypeTree]) { self =>
    def isEmpty: Boolean =
      self.types.isEmpty && self.obj.isEmpty && self.list.isEmpty
    def insert(steps: List[Step], insertee: TypeTree): TypeTree = steps match {
      case List() =>
        TypeTree(insertee.types ++ self.types, self.obj ++ insertee.obj, self.list ++ insertee.list)
      case child :: tail => child match {
        case Field(str) =>
          val insertTo = self.obj.get(str) getOrElse emptyTree
          val inserted = insertTo.insert(tail, insertee)
          self.copy(obj = self.obj.updated(str, inserted))
        case Index(ix) =>
          val insertTo = self.list.lift(ix) getOrElse emptyTree
          val inserted = insertTo.insert(tail, insertee)
          self.copy(list = self.list.updated(ix, inserted))
      }
    }
  }
  val emptyTree = TypeTree(Set.empty, Map.empty, Map.empty)

  object TypeTree {
    def fromMask(mapper: Mapper, masks: Map[CPath, Set[ColumnType]]): Option[TypeTree] = {
      masks.toList.foldLeftM(emptyTree) { case (acc, (cpath, types)) =>
        Projection.fromCPath(cpath)
          .map(mapper.projection)
          .map(p => acc.insert(p.steps, TypeTree(types, Map.empty, Map.empty)))
      }
    }
  }
  private def parseTypeStrings(parseType: ColumnType): List[String] = parseType match {
    case ColumnType.Boolean =>
      List("bool")
    case ColumnType.Null =>
      List("null")
    case ColumnType.Number =>
      List("double", "long", "int", "decimal")
    case ColumnType.String =>
      List("string", "objectId", "binData", "dbPointer", "symbol", "javascript", "javascriptWithScope", "regex")
    case ColumnType.OffsetDateTime =>
      List("date")
    case ColumnType.Array =>
      List("array")
    case ColumnType.Object =>
      List("object")
    case _ => List()
  }


  private def typeTreeFilters[U: Basis[Expr, ?]](proj: Projection, tree: TypeTree): U = {
    val o = Optics.full(Optics.basisPrism[Expr, U])
    if (tree.types === ColumnType.Top) {
      o.not(o.eqx(List(
        o.typ(o.projection(proj)),
        o.str("missing"))))
    } else {
      val typeExprs: List[U] = tree.types.toList.flatMap(parseTypeStrings(_)).map(o.str(_))
      val eqExprs: List[U] = typeExprs.map(x => o.eqx(List(o.typ(o.projection(proj)), x)))
      val listExprs: List[U] =
        if (tree.types.contains(ColumnType.Array)) List()
        else tree.list.toList.sortBy(_._1) map {
          case (i, child) => typeTreeFilters(proj + Projection.index(i), child)
        }
      val objExprs: List[U] =
        if (tree.types.contains(ColumnType.Object)) List()
        else tree.obj.toList map {
          case (key, child) => typeTreeFilters(proj + Projection.key(key), child)
        }
      o.or(eqExprs ++ listExprs ++ objExprs)
    }
  }

  final case class Config[U](uniqueKey: String, undefined: U, mapper: Mapper)

  def inArray[F[_]: ApplicativeLocal[?[_], Config[U]], U: Basis[Expr, ?]](action: F[U]): F[U] = {
    val modify: Config[U] => Config[U] = cfg => cfg.copy(undefined = missing[Expr, U](cfg.uniqueKey))
    ApplicativeLocal[F, Config[U]].local(modify)(action)
  }

  def inObject[F[_]: ApplicativeLocal[?[_], Config[U]], U: Basis[Expr, ?]](action: F[U]): F[U] = {
    val modify: Config[U] => Config[U] = cfg => cfg.copy(undefined = missingKey[Expr, U](cfg.uniqueKey))
    ApplicativeLocal[F, Config[U]].local(modify)(action)
  }

  private def rebuildDoc[F[_]: Monad[?[_]]: ApplicativeLocal[?[_], Config[U]], U: Basis[Expr, ?]](proj: Projection, tree: TypeTree): F[U] =
    ApplicativeAsk[F, Config[U]].ask flatMap { case Config(uniqueKey, undefined, mapper) =>
      val o = Optics.full(Optics.basisPrism[Expr, U])
      val obj: F[U] = {
        val treeMap = tree.obj.toList.foldLeftM(Map[String, U]()) {
          case (m, (key, child)) =>
            rebuildDoc[F, U](proj + Projection.key(key), child) map (m.updated(key, _))
        }
        treeMap.map(o.obj(_))
      }
      val array: F[U] = inArray[F, U] {
        for {
          arr <- tree.list.toList.sortBy(_._1) traverse {
            case (i, child) => rebuildDoc[F, U](proj + Projection.index(i), child)
          }
          undef <- ApplicativeAsk[F, Config[U]].ask map (_.undefined)
        } yield
          o.reduce(
            o.array(arr),
            o.array(List()),
            o.cond(
              o.eqx(List(o.str("$$this"), undef)),
              o.str("$$value"),
              o.concatArrays(List(
                o.str("$$value"),
                o.array(List(o.str("$$this")))))))
      }
      val isArray: U => U = { e  => o.eqx(List(o.typ(e), o.str("array"))) }
      val isObject: U => U = { e =>  o.eqx(List(o.typ(e), o.str("object"))) }

      def arrayOr(e: U): F[U] =
        if (tree.types.contains(ColumnType.Array) || tree.list.isEmpty) e.pure[F]
        else array.map(o.cond(isArray(o.projection(proj)), _, e))
      def objectOr(e: U): F[U] =
        if (tree.types.contains(ColumnType.Object) || tree.obj.isEmpty) e.pure[F]
        else inObject(obj).map(o.cond(isObject(o.projection(proj)), _, e))
      def projectionOr(e: U): U =
        if (tree.types.isEmpty) e else o.projection(proj)

      def rootWrapper(x: F[U]): F[U] = {
        if (proj === Projection.key(uniqueKey)) mapper match {
          case Mapper.Focus(key) if key === uniqueKey => inArray[F, U](x)
          case _ => x
        } else x
      }

      if (proj === Projection(List())) {
        if (tree.obj.isEmpty) {
          if (tree.types.contains(ColumnType.Object)) o.str("$$ROOT").pure[F]
          else undefined.pure[F]
        } else if (tree.obj.keySet === Set(uniqueKey) && mapper === Mapper.Focus(uniqueKey)) {
          inArray(obj)
        } else {
          inObject(obj)
        }
      } else rootWrapper {
        for {
          obj <- objectOr(projectionOr(undefined))
          arr <- arrayOr(obj)
        } yield o.cond(typeTreeFilters(proj, tree), arr, undefined)
      }
    }

    def apply[F[_]: Monad: MonoidK: MonadInState, U: Basis[Expr, ?]]
        : Interpreter[F, U, ScalarStage.Mask] = new Interpreter[F, U, ScalarStage.Mask] {
      def apply(s: ScalarStage.Mask) = s match { case ScalarStage.Mask(masks) =>
        MonadState[F, InterpretationState].get flatMap { state =>
          val eraseId = Map("_id" -> o.int(0))
          def projectionObject(tree: TypeTree): Option[Map[String, U]] = {
            val initialConfig = Config(state.uniqueKey, missing[Expr, U](state.uniqueKey), state.mapper)
            val rebuilt = rebuildDoc[Config[U] => ?, U](Projection(List()), tree).apply(initialConfig)
            state.mapper match {
              case Mapper.Unfocus => Some(Map(state.uniqueKey -> rebuilt))
              case Mapper.Focus(_) => o.obj.getOption(rebuilt)
            }
          }
          if (masks.isEmpty) {
            List(Pipeline.Erase: Pipeline[U]).pure[F]
          }
          else for {
            projObj <- optToAlternative[F].apply(for {
              tree <- TypeTree.fromMask(state.mapper, masks)
              pObj <- projectionObject(tree)
            } yield pObj)
            _ <- focus[F]
          } yield List(Pipeline.Project(eraseId ++ projObj), Pipeline.Presented)
        }
      }
    }
}
