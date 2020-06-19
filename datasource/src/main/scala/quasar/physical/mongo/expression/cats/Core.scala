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

package quasar.physical.mongo.expression.cats

import slamdata.Predef.{Eq => _, _}

import quasar.{ScalarStage, ScalarStages}
import quasar.common.{CPath, CPathField, CPathIndex}
import quasar.common.CPath
import quasar.api.ColumnType
import quasar.physical.mongo.{PushdownLevel, Version}

import _root_.iota.TListK.:::
import _root_.iota.{CopK, TListK, TNilK}
import _root_.iota.syntax._

import cats._
import cats.data._
import cats.implicits._
import cats.mtl._
import cats.mtl.implicits._
import higherkindness.droste._
import higherkindness.droste.data._
import higherkindness.droste.syntax.all._
import higherkindness.droste.syntax.compose._
import higherkindness.droste.util.DefaultTraverse

import monocle.{Iso, Prism}
import monocle.macros.{GenLens, GenPrism}

import org.bson._
import quasar.{ScalarStage, IdStatus}
import quasar.physical.mongo.Version

object iota {
  type ACopK[α] = CopK[_, α]
  type :<<:[F[_], G[α] <: ACopK[α]] = CopK.Inject[F, G]

  def mkInject[F[_], LL <: TListK](i: Int): CopK.Inject[F, CopK[LL, ?]] =
    CopK.Inject.injectFromInjectL[F, LL](
      CopK.InjectL.makeInjectL[F, LL](
        new TListK.Pos[LL, F] { val index = i }))

  object functor {
    sealed trait FunctorMaterializer[LL <: TListK] {
      def materialize(offset: Int): Functor[CopK[LL, ?]]
    }
    implicit def base[F[_]: Functor]: FunctorMaterializer[F ::: TNilK] = new FunctorMaterializer[F ::: TNilK] {
      def materialize(offset: Int): Functor[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new Functor[CopK[F ::: TNilK, ?]] {
          def map[A, B](cfa: CopK[F ::: TNilK, A])(f: A => B): CopK[F ::: TNilK, B] = cfa match {
            case I(fa) => I(fa map f)
          }
        }
      }
    }
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_]: Functor, LL <: TListK](
        implicit LL: FunctorMaterializer[LL])
        : FunctorMaterializer[F ::: LL] = new FunctorMaterializer[F ::: LL] {
      def materialize(offset: Int): Functor[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new Functor[CopK[F ::: LL, ?]] {
          def map[A, B](cfa: CopK[F ::: LL, A])(f: A => B): CopK[F ::: LL, B] = cfa match {
            case I(fa) => I(fa map f)
            case other => LL.materialize(offset + 1).map(other.asInstanceOf[CopK[LL, A]])(f).asInstanceOf[CopK[F ::: LL, B]]
          }
        }
      }
    }
  }

  object eq {
    sealed trait DelayEqMaterializer[LL <: TListK] {
      def materialize(offset: Int): Delay[Eq, CopK[LL, ?]]
    }

    implicit def base[F[_]](implicit F: Delay[Eq, F]): DelayEqMaterializer[F ::: TNilK] = new DelayEqMaterializer[F ::: TNilK] {
      def materialize(offset: Int): Delay[Eq, CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new (Eq ~> λ[α => Eq[CopK[F ::: TNilK, α]]]) {
          def apply[A](eqa: Eq[A]): Eq[CopK[F ::: TNilK, A]] = new Eq[CopK[F ::: TNilK, A]] {
            def eqv(a: CopK[F ::: TNilK, A], b: CopK[F ::: TNilK, A]): Boolean = (a, b) match {
              case (I(left), I(right)) => F(eqa).eqv(left, right)
              case _ => false
            }
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_], LL <: TListK](
        implicit
        F: Delay[Eq, F],
        LL: DelayEqMaterializer[LL])
        : DelayEqMaterializer[F ::: LL] = new DelayEqMaterializer[F ::: LL] {
      def materialize(offset: Int): Delay[Eq, CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new (Eq ~> λ[α => Eq[CopK[F ::: LL, α]]]) {
          def apply[A](eqa: Eq[A]): Eq[CopK[F ::: LL, A]] = new Eq[CopK[F ::: LL, A]] {
            def eqv(a: CopK[F ::: LL, A], b: CopK[F ::: LL, A]) = (a, b) match {
              case ((I(left), I(right))) => F(eqa).eqv(left, right)
              case (left, right) => LL.materialize(offset + 1)(eqa).eqv(
                left.asInstanceOf[CopK[LL, A]],
                right.asInstanceOf[CopK[LL, A]])
            }
          }
        }
      }
    }
  }

  object traverse {
    sealed trait TraverseMaterializer[LL <: TListK] {
      def materialize(offset: Int): Traverse[CopK[LL, ?]]
    }

    implicit def base[F[_]: Traverse]: TraverseMaterializer[F ::: TNilK] = new TraverseMaterializer[F ::: TNilK] {
      def materialize(offset: Int): Traverse[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new DefaultTraverse[CopK[F ::: TNilK, ?]] {
          def traverse[G[_]: Applicative, A, B](cfa: CopK[F ::: TNilK, A])(f: A => G[B]): G[CopK[F ::: TNilK, B]] = cfa match {
            case I(fa) => fa.traverse(f).map(I(_))
          }
        }
      }
    }
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_]: Traverse, LL <: TListK](
        implicit LL: TraverseMaterializer[LL])
        : TraverseMaterializer[F ::: LL] = new TraverseMaterializer[F ::: LL] {
      def materialize(offset: Int): Traverse[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new DefaultTraverse[CopK[F ::: LL, ?]] {
          def traverse[G[_]: Applicative, A, B](cfa: CopK[F ::: LL, A])(f: A => G[B]): G[CopK[F ::: LL, B]] = cfa match {
            case I(fa) => fa.traverse(f).map(I(_))
            case other => LL.materialize(offset + 1).traverse(other.asInstanceOf[CopK[LL, A]])(f).asInstanceOf[G[CopK[F ::: LL, B]]]
          }
        }
      }
    }
  }


  import functor._
  import eq._
  import traverse._

  implicit def copkFunctor[LL <: TListK](implicit M: FunctorMaterializer[LL]): Functor[CopK[LL, ?]] = M.materialize(offset = 0)
  implicit def copkEq[LL <: TListK](implicit M: DelayEqMaterializer[LL]): Delay[Eq, CopK[LL, ?]] = M.materialize(offset = 0)
  implicit def copkTraverse[LL <: TListK](implicit M: TraverseMaterializer[LL]): Traverse[CopK[LL, ?]] = M.materialize(offset = 0)
}


sealed trait Core[A] extends Product with Serializable

object Core {
  final case class Array[A](value: List[A]) extends Core[A]
  final case class Object[A](value: Map[String, A]) extends Core[A]
  final case class Bool[A](value: Boolean) extends Core[A]
  final case class SInt[A](value: Int) extends Core[A]
  final case class SString[A](value: String) extends Core[A]
  final case class Null[A]() extends Core[A]

  type Fixed = Fix[Core]

  implicit def traverse: Traverse[Core] = new DefaultTraverse[Core] {
    def traverse[G[_]: Applicative, A, B](fa: Core[A])(f: A => G[B]): G[Core[B]] = fa match {
      case Array(v) => v.traverse(f).map(Array(_))
      case Object(v) => v.toList.traverse(_.traverse(f)).map(x => Object(x.toMap))
      case Bool(v) => (Bool(v): Core[B]).pure[G]
      case SInt(v) => (SInt(v): Core[B]).pure[G]
      case SString(v) => (SString(v): Core[B]).pure[G]
      case Null() => (Null(): Core[B]).pure[G]
    }
  }

  trait Optics[A, O] {
    val core: Prism[O, Core[A]]

    val _array =
      Prism.partial[Core[A], List[A]]{case Array(v) => v}(Array(_))
    val _obj =
      Prism.partial[Core[A], Map[String, A]]{case Object(v) => v}(Object(_))
    val _bool =
      Prism.partial[Core[A], Boolean]{case Bool(v) => v}(Bool(_))
    val _int =
      Prism.partial[Core[A], Int]{case SInt(v) => v}(SInt(_))
    val _str =
      Prism.partial[Core[A], String]{case SString(v) => v}(SString(_))
    val _nil =
      Prism.partial[Core[A], Unit]{case Null() => ()}(x => Null())

    val array = core composePrism _array
    val obj = core composePrism _obj
    val bool = core composePrism _bool
    val int = core composePrism _int
    val str = core composePrism _str
    val nil = core composePrism _nil
  }
  def optics[A, O](bp: Prism[O, Core[A]]) = new Optics[A, O] { val core = bp }
}

sealed trait Op[A] extends Product with Serializable {
  def minVersion: Version = Version.zero
}

object Op {
  final case class Let[A](vars: Map[String, A], in: A) extends Op[A]
  final case class Not[A](exp: A) extends Op[A]
  final case class Eq[A](exps: List[A]) extends Op[A]
  final case class Or[A](exps: List[A]) extends Op[A]
  final case class Exists[A](exp: A) extends Op[A]
  final case class Cond[A](check: A, ok: A, fail: A) extends Op[A]
  final case class Ne[A](exp: A) extends Op[A]
  final case class Type[A](exp: A) extends Op[A] {
    override def minVersion = Version.$type
  }
  final case class ObjectToArray[A](exp: A) extends Op[A] {
    override def minVersion = Version.$objectToArray
  }
  final case class ArrayElemAt[A](exp: A, ix: Int) extends Op[A] {
    override def minVersion = Version.$arrayElemAt
  }
  final case class Reduce[A](input: A, initialValue: A, expression: A) extends Op[A] {
    override def minVersion = Version.$reduce
  }
  final case class ConcatArrays[A](exps: List[A]) extends Op[A] {
    override def minVersion = Version.$concatArrays
  }

  implicit def traverse: Traverse[Op] = new DefaultTraverse[Op] {
    def traverse[G[_]: Applicative, A, B](fa: Op[A])(f: A => G[B]): G[Op[B]] = fa match {
      case ArrayElemAt(x, i) => f(x).map(ArrayElemAt(_, i))
      case ObjectToArray(x) => f(x).map(ObjectToArray(_))
      case Ne(x) => f(x).map(Ne(_))
      case Exists(x) => f(x).map(Exists(_))
      case Cond(a, b, c) => (f(a), f(b), f(c)).mapN(Cond(_, _, _))
      case Or(lst) => lst traverse f map (Or(_))
      case Eq(lst) => lst traverse f map (Eq(_))
      case Type(x) => f(x) map (Type(_))
      case Let(vars, in) =>  (vars.toList.traverse(_.traverse(f)).map(_.toMap), f(in)).mapN(Let(_, _))
      case Reduce(a, b, c) => (f(a), f(b), f(c)).mapN(Reduce(_, _, _))
      case ConcatArrays(lst) => lst traverse f map (ConcatArrays(_))
      case Not(a) => f(a) map (Not(_))
    }
  }

  trait Optics[A, O] {
    val op: Prism[O, Op[A]]

    val _let =
      Prism.partial[Op[A], (Map[String, A], A)]{case Let(a, b) => (a, b)}{case (a, b) => Let(a, b)}
    val _type =
      Prism.partial[Op[A], A]{case Type(v) => v}(Type(_))
    val _eq =
      Prism.partial[Op[A], List[A]]{case Eq(v) => v}(Eq(_))
    val _or =
      Prism.partial[Op[A], List[A]]{case Or(v) => v}(Or(_))
    val _exists =
      Prism.partial[Op[A], A]{case Exists(v) => v}(Exists(_))
    val _cond =
      Prism.partial[Op[A], (A, A, A)]{case Cond(a, b, c) => (a, b, c)}{case (a, b, c) => Cond(a, b, c)}
    val _ne =
      Prism.partial[Op[A], A]{case Ne(v) => v}(Ne(_))
    val _objectToArray =
      Prism.partial[Op[A], A]{case ObjectToArray(v) => v}(ObjectToArray(_))
    val _arrayElemAt =
      Prism.partial[Op[A], (A, Int)]{case ArrayElemAt(a, b) => (a, b)}{case (a, b) => ArrayElemAt(a, b)}
    val _reduce =
      Prism.partial[Op[A], (A, A, A)]{case Reduce(a, b, c) => (a, b, c)}{case (a, b, c) => Reduce(a, b, c)}
    val _concatArrays =
      Prism.partial[Op[A], List[A]]{case ConcatArrays(v) => v}(ConcatArrays(_))
    val _not =
      Prism.partial[Op[A], A]{case Not(v) => v}(Not(_))

    val let = op composePrism _let
    val typ = op composePrism _type
    val eqx = op composePrism _eq
    val or = op composePrism _or
    val exists = op composePrism _exists
    val cond = op composePrism _cond
    val nex = op composePrism _ne
    val objectToArray = op composePrism _objectToArray
    val arrayElemAt = op composePrism _arrayElemAt
    val reduce = op composePrism _reduce
    val concatArrays = op composePrism _concatArrays
    val not = op composePrism _not
  }

  def optics[A, O](bp: Prism[O, Op[A]]): Optics[A, O] = new Optics[A, O] { val op = bp }
}

final case class Projection(steps: List[Projection.Step]) {
  def +(prj: Projection): Projection = Projection(steps ++ prj.steps)
  def toKey: String = steps match {
    case List() => "$$ROOT"
    case hd :: tail => tail.foldLeft(hd.keyPart){(acc, x) => s"$acc.${x.keyPart}"}
  }
}

object Projection {
  trait Step extends Product with Serializable {
    def keyPart: String
  }
  object Step {
    final case class Field(name: String) extends Step {
      def keyPart: String = name
    }
    final case class Index(ix: Int) extends Step {
      def keyPart: String = ix.toString
    }

    implicit val order: Order[Step] = new Order[Step] {
      def compare(a: Step, b: Step): Int = (a, b) match {
        case (Field(_), Index(_)) => -1
        case (Field(a), Field(b)) => Order[String].compare(a, b)
        case (Index(_), Field(_)) => 1
        case (Index(a), Index(b)) => Order[Int].compare(a, b)
        case (_, _) => -1
      }
    }
  }
  import Step._
  implicit val order: Order[Projection] = Order.by(_.steps)

  def key(s: String): Projection = Projection(List(Field(s)))
  def index(i: Int): Projection = Projection(List(Index(i)))

  def safeField(str: String): Option[Field] =
    if (str.contains(".") || str.contains("$")) None
    else Some(Field(str))

  def safeCartouches[A](inp: Map[CPathField, (CPathField, A)]): Option[Map[Field, (Field, A)]] =
    inp.toList.foldLeftM(Map.empty[Field, (Field, A)]){ (acc, x) =>
      x match {
        case (alias, (focus, lst)) => for {
          newAlias <- safeField(alias.name)
          newFocus <- safeField(focus.name)
        } yield acc.updated(newAlias, (newFocus, lst))
      }
    }

  def fromCPath(cpath: CPath): Option[Projection] =
    cpath.nodes.foldMapM {
      case CPathField(str) => safeField(str).map(List(_))
      case CPathIndex(ix) => Some(List(Index(ix)))
      case _ => None
    } map (Projection(_))

  trait Grouped extends Product with Serializable

  object Grouped {
    final case class IndexGroup(i: Int) extends Grouped
    final case class FieldGroup(s: List[String]) extends Grouped

    def apply(p: Projection): List[Grouped] = {
      val accum = p.steps.foldLeft((List[String](), List[Grouped]())) { (acc, step) =>
        acc match {
          case (fldAccum, acc) => step match {
            case Field(s) => (s :: fldAccum, acc)
            case Index(i) => (List(), IndexGroup(i) :: FieldGroup(fldAccum.reverse) :: acc)
          }
        }
      }
      accum._1 match {
        case List() => accum._2.reverse
        case x => (FieldGroup(x.reverse) :: accum._2).reverse
      }
    }
  }

  trait Optics[A, O] {
    val proj: Prism[O, Const[Projection, A]]

    val _projection =
      Iso[Const[Projection, A], Projection](_.getConst)(Const(_))
    val projection =
      proj composePrism _projection.asPrism

    val _steps =
      Iso[Projection, List[Step]](_.steps)(Projection(_))

    val steps =
      projection composeIso _steps

    val key: Prism[O, String] =
      steps composePrism {
        Prism.partial[List[Step], String] {
          case Field(s) :: List() => s
        } { s => List(Field(s)) }
      }
  }

  def optics[A, O](bp: Prism[O, Const[Projection, A]]): Optics[A, O] = new Optics[A, O] { val proj = bp }
}


object optics0 {
  import iota._
  import monocle.Prism



  def copkPrism[F[_], G[a] <: ACopK[a], A](implicit I: F :<<: G): Prism[G[A], F[A]] =
    Prism[G[A], F[A]]((x: G[A]) => I.prj(x))((x: F[A]) => I.inj(x))

  def basisIso[F[_], U: Basis[F, ?]]: Iso[U, F[U]] =
    Iso((x: U) => x.project)((x: F[U]) => x.embed)

  def basisPrism[F[_], U](implicit basis: Basis[F, U]): Prism[U, F[U]] =
    basisIso.asPrism

  def coattrFPrism[F[_], A, B] =
    Prism.partial[CoattrF[F, A, B], F[B]]{case CoattrF.Roll(f) => f}(CoattrF.roll(_))

  def core[F[a] <: ACopK[a], A, O](bp: Prism[O, F[A]])(implicit c: Core :<<: F): Core.Optics[A, O] = new Core.Optics[A, O] {
    val core = bp composePrism copkPrism[Core, F, A]
  }

  def coreOp[F[a] <: ACopK[a], A, O](bp: Prism[O, F[A]])(implicit c: Core :<<: F, o: Op :<<: F): Core.Optics[A, O] with Op.Optics[A, O] =
    new Core.Optics[A, O] with Op.Optics[A, O] {
      val core = bp composePrism copkPrism[Core, F, A]
      val op = bp composePrism copkPrism[Op, F, A]
    }

  def full[F[a] <: ACopK[a], A, O](bp: Prism[O, F[A]])(
      implicit
      c: Core :<<: F,
      o: Op :<<: F,
      p: Const[Projection, ?] :<<: F)
      : Core.Optics[A, O] with Op.Optics[A, O] with Projection.Optics[A, O] =
  new Core.Optics[A, O] with Op.Optics[A, O] with Projection.Optics[A, O] { self =>
    val core = bp composePrism copkPrism[Core, F, A]
    val op = bp composePrism copkPrism[Op, F, A]
    val proj = bp composePrism copkPrism[Const[Projection, ?], F, A]
  }
}
object algebras {
  type CoreOpL = Core ::: Op ::: TNilK
  type ExprL = Const[Projection, ?] ::: Core ::: Op ::: TNilK
  type CoreOp[A] = CopK[Core ::: Op ::: TNilK, A]
  type Core0[A] = CopK[Core ::: TNilK, A]
  type Expr[A] = CopK[Const[Projection, ?] ::: Core ::: Op ::: TNilK, A]
}

import algebras._

object example {
  import iota._
  import Core._
  import Op._
  import optics0._

  def opsMinVersion: Algebra[Op, Version] = {
    val order = Order[Version]
    import order._
    Algebra { x =>
      val fMin = x.minVersion
      x match {
        case Op.Let(vars, in) =>
          max(vars.toList.map(_._2).foldLeft(in)(max), fMin)
        case Op.Type(a) =>
          max(fMin, a)
        case Op.Eq(a) =>
          a.foldLeft(fMin)(max)
        case Op.Or(a) =>
          a.foldLeft(fMin)(max)
        case Op.Cond(a, b, c) =>
          List(a, b, c).foldLeft(fMin)(max)
        case Op.Ne(a) =>
          max(fMin, a)
        case Op.ObjectToArray(a) =>
          max(fMin, a)
        case Op.ArrayElemAt(a, _) =>
          max(fMin, a)
        case Op.Reduce(a, b ,c) =>
          List(a, b, c).foldLeft(fMin)(max)
        case Op.ConcatArrays(a) =>
          a.foldLeft(fMin)(max)
        case Op.Not(a) =>
          max(fMin, a)
      }
    }
  }

  def coreOpsVersion: Algebra[CoreOp, Version] =
    Algebra{ (x: CoreOp[Version]) =>
      CopK.RemoveL[Op, Core ::: Op ::: TNilK].apply(x) match {
        case Left(a) => Version.zero
        case Right(b) => opsMinVersion(b)
      }
    }

  def coreOpsVersionRun[W: Basis[CoreOp, ?]]: W => Version =
    scheme.cata[CoreOp, W, Version](coreOpsVersion)

  def opsToCore[U: Basis[Core0, ?]]: Algebra[Op, U] = {
    val core0 = optics0.core(basisPrism[Core0, U])
    import core0._
    Algebra {
      case Op.Let(vars, in) => obj(Map("$let" -> obj(Map("vars" -> obj(vars), "in" -> in))))
      case Op.Type(a) => obj(Map("$type" -> a))
      case Op.Eq(a) => obj(Map("$eq" -> array(a)))
      case Op.Or(a) => obj(Map("$or" -> array(a)))
      case Op.Exists(a) => obj(Map("$exists" -> a))
      case Op.Cond(a, b, c) => obj(Map("$cond" -> array(List(a, b, c))))
      case Op.Ne(a) => obj(Map("$ne" -> a))
      case Op.ObjectToArray(a) => obj(Map("$objectToArray" -> a))
      case Op.ArrayElemAt(a, ix) => obj(Map("$arrayElemAt" -> array(List(a, int(ix)))))
      case Op.Reduce(a, b, c) => obj(Map("$reduce" -> obj(Map(
        "input" -> a,
        "initialValue" -> b,
        "in" -> c))))
      case Op.ConcatArrays(a) => obj(Map("$concatArrays" -> array(a)))
      case Op.Not(a) => obj(Map("$not" -> a))
    }
  }

  def compute[LL <: TListK, RR <: TListK, A](inp: CopK[LL, A])(implicit compute: TListK.Compute.Aux[LL, RR]): CopK[RR, A] =
    inp.asInstanceOf[CopK[RR, A]]

  def coreOpsToCore[U: Basis[Core0, ?]]: Algebra[CoreOp, U] = {
    Algebra { (x: CoreOp[U]) =>
      CopK.RemoveL[Op, Core ::: Op ::: TNilK].apply(x) match {
        case Left(a) => compute(a).embed
        case Right(b) => opsToCore.run(b)
      }
    }
  }

  def coreOpsToCore0Run[W: Basis[CoreOp, ?], U: Basis[Core0, ?]]: W => U =
    scheme.cata[CoreOp, W, U](coreOpsToCore)

  def core0ToCore[W: Basis[Core0, ?], U: Basis[Core, ?]]: W => U =
    scheme.cata[Core0, W, U](Algebra { x =>
      CopK.RemoveL[Core, Core ::: TNilK].apply(x) match {
        case Left(_) => throw new Throwable("impossible")
        case Right(a) => a.embed
      }
    })

  val MissingSuffix = "_missing"
  type Elem = (Projection.Grouped, Int)
  import Projection.Grouped._

  def unfoldProjectionψ(uuid: String): CVCoalgebra[CoreOp, List[Elem]] = {
    type U = Coattr[CoreOp, List[Elem]]
    type F[X] = CoattrF[CoreOp, List[Elem], X]

    val toCopK = basisPrism[F, U] composePrism coattrFPrism[CoreOp, List[Elem], U]
    val ff = optics0.coreOp(Prism.id: Prism[CoreOp[U], CoreOp[U]])
    val coreOp = optics0.coreOp(toCopK)
    import coreOp._

    CVCoalgebra {
      case List() =>
        ff.str("$$ROOT")
      case (FieldGroup(hd :: tail), _) :: List() =>
        ff.str(tail.foldLeft("$".concat(hd)){ (acc, s) => s"$acc.$s" })
      case (hd, levelIx) :: tl =>
        val level = s"level${levelIx.toString}"
        val levelExp = str("$$".concat(level))
        val tailCont: U = Coattr.pure(tl)
        val undefined = str(s"$uuid$MissingSuffix")
        hd match {
          case IndexGroup(i) =>
            ff.let(
              Map(level -> tailCont),
              cond(
                eqx(List(typ(tailCont), str("array"))),
                arrayElemAt(str("$$".concat(level)), i),
                undefined))
          case FieldGroup(steps) =>
            val expSteps = FieldGroup("$".concat(level) :: steps)
            val exp: U = Coattr.pure(List((expSteps, 0)))
            ff.let(
              Map(level -> tailCont),
              exp)
      }
    }
  }

  def unfoldProjection[U: Basis[CoreOp, ?]](uuid: String): Projection => U = { (prj: Projection) =>
    val grouped = Projection.Grouped(prj).zipWithIndex
    val fn: List[Elem] => U = scheme.zoo.futu(unfoldProjectionψ(uuid))
    fn(grouped)
  }

  sealed trait Pipeline[+A] extends Product with Serializable
  sealed trait MongoPipeline[+A] extends Pipeline[A] {
    def minVersion = Version.zero
  }
  sealed trait CustomPipeline extends Pipeline[Nothing]

  object Pipeline {
    final case class Project[A](obj: Map[String, A]) extends MongoPipeline[A]
    final case class Match[A](a: A) extends MongoPipeline[A]
    final case class Unwind(path: String, arrayIndex: String) extends MongoPipeline[Nothing] {
      override def minVersion = Version.$unwind
    }


    final case object Presented extends CustomPipeline
    final case object Erase extends CustomPipeline

    implicit val functor: Functor[Pipeline] = new Functor[Pipeline] {
      def map[A, B](fa: Pipeline[A])(f: A => B): Pipeline[B] = fa match {
        case Presented => Presented
        case Erase => Erase
        case Project(obj) => Project(obj.toList.map({case (k, v) => (k, f(v))}).toMap)
        case Match(a) => Match(f(a))
        case Unwind(p, i) => Unwind(p, i)
      }
    }
  }

  def missing[F[a] <: ACopK[a], U](k: String)(implicit I: Core :<<: F, P: Basis[F, U]): U =
    optics0.core(basisPrism[F, U]).str(k.concat(MissingSuffix))

  def missingKey[F[a] <: ACopK[a], U](key: String)(implicit I: Core :<<: F, U: Basis[F, U]): U =
    optics0.core(basisPrism[F, U]).str("$".concat(key).concat(MissingSuffix))

  import Pipeline._
  def eraseCustomPipeline[U: Basis[Expr, ?]](uuid: String, pipeline: Pipeline[U]): List[MongoPipeline[U]] = {
    val o = optics0.full(basisPrism[Expr, U])
    import o._

    pipeline match {
      case Presented =>
        List(Match(obj(Map(uuid -> nex(missing[Expr, U](uuid))))))
      case Erase =>
        List(Match(obj(Map(uuid.concat("_erase") -> bool(false)))))
      case Project(obj) =>
        List(Project(obj))
      case Match(obj) =>
        List(Match(obj))
      case Unwind(a, i) =>
        List(Unwind(a, i))
    }
  }

  def pipelineObjects[F[a] <: ACopK[a], U](pipe: MongoPipeline[U])(implicit I: Core :<<: F, U: Basis[F, U]): U = {
    val o = optics0.core(basisPrism[F, U])
    pipe match {
      case Match(mp) =>
        o.obj(Map("$match" -> mp))
      case Project(mp) =>
        o.obj(Map("$project" -> o.obj(mp)))
      case Unwind(path, arrayIndex) =>
        o.obj(Map("$unwind" -> o.obj(Map(
          "path" -> o.str("$".concat(path)),
          "includeArrayIndex" -> o.str(arrayIndex),
          "preserveNullAndEmptyArrays" -> o.bool(false)))))
    }
  }

  def letOpt[W: Basis[CoreOp, ?], U: Basis[CoreOp, ?]]: W => U = {
    val o = optics0.coreOp(basisPrism[CoreOp, U])
    val f = optics0.coreOp(Prism.id[CoreOp[U]])
    import o._
    scheme.cata[CoreOp, W, U](Algebra {
      case f.let(vars, in) =>
        if (vars.size =!= 1) let(vars, in)
        else in match {
          case str(k) =>
            vars.get(k.stripPrefix("$$")) match {
              case Some(str(v)) if v.stripPrefix("$") =!= v =>
                str(v.concat(v))
              case _ =>
                let(vars, in)
            }
        }
        let(vars, in)
      case x => x.embed
    })
  }
  def orOpt[W: Basis[CoreOp, ?], U: Basis[CoreOp, ?]]: W => U = {
    val o = optics0.coreOp(basisPrism[CoreOp, U])
    val f = optics0.coreOp(Prism.id[CoreOp[U]])
    import o._
    scheme.cata[CoreOp, W, U](Algebra {
      case f.or(lst) => lst match {
        case List() => bool(false)
        case List(a) => a
        case _ => or(lst flatMap {
          case or(as) => as
          case x => List(x)
        })
      }
    })
  }
  def mapProjection[F[a] <: ACopK[a], W, U](fn: Projection => Projection)(
      implicit
      F: Functor[F],
      I: Const[Projection, ?] :<<: F,
      Q: Basis[F, W],
      P: Basis[F, U])
      : W => U = {
    val o = Projection.optics(basisPrism[F, U] composePrism copkPrism[Const[Projection, ?], F, U])
    val f = Projection.optics(copkPrism[Const[Projection, ?], F, U])
    scheme.cata[F, W, U](Algebra {
      case f.projection(prj) =>
        o.projection(fn(prj))
      case x => x.embed
    })
  }
}


sealed trait Mapper extends Product with Serializable { self =>
  import Mapper._

  def inBson(inp: BsonValue): BsonValue = self match {
    case Unfocus => inp
    case Focus(str) => inp.asDocument().get(str)
  }
  def inProjection(inp: Projection): Projection = self match {
    case Unfocus => inp
    case Focus(str) => Projection.key(str) + inp
  }
}

object Mapper {
  final case class Focus(str: String) extends Mapper
  final case object Unfocus extends Mapper

  implicit val order: Order[Mapper] = new Order[Mapper] {
    def compare(a: Mapper, b: Mapper): Int = a match {
      case Unfocus => b match {
        case Unfocus => 0
        case Focus(_) => -1
      }
      case Focus(afld) => b match {
        case Unfocus => 1
        case Focus(bfld) => Order[String].compare(afld, bfld)
      }
    }
  }
}

object state {
  final case class InterpretationState(uniqueKey: String, mapper: Mapper)
  type InState[A] = StateT[Option, InterpretationState, A]

  def focus[F[_]: MonadState[?[_], InterpretationState]]: F[Unit] =
    MonadState[F, InterpretationState].modify { x => x.copy(mapper = Mapper.Focus(x.uniqueKey)) }

  def unfocus[F[_]: MonadState[?[_], InterpretationState]]: F[Unit] =
    MonadState[F, InterpretationState].modify { x => x.copy(mapper = Mapper.Unfocus) }
}


object interpreter {
  import example._
  trait Interpreter[F[_], U, -S] {
    def optics(implicit U: Basis[Expr, U]) = optics0.full(optics0.basisPrism[Expr, U])
    def o(implicit U: Basis[Expr, U]) = optics
    def apply(s: S): F[List[Pipeline[U]]]
  }

  object Interpreter {
    def apply[F[_], U: Basis[Expr, ?], S](f: S => F[List[Pipeline[U]]]): Interpreter[F, U, S] = new Interpreter[F, U, S] {
      def apply(s: S): F[List[Pipeline[U]]] = f(s)
    }
  }

  def optToAlternative[F[_]: Applicative: MonoidK]: Option ~> F = new (Option ~> F) {
    def apply[A](inp: Option[A]): F[A] = inp match {
      case None => MonoidK[F].empty
      case Some(a) => a.pure[F]
    }
  }

  object Mask {
    import Projection._
    import Projection.Step._

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
            .map(mapper.inProjection(_))
            .map(p => acc.insert(p.steps, TypeTree(types, Map.empty, Map.empty)))
        }
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


    import optics0._

    private def typeTreeFilters[U: Basis[Expr, ?]](proj: Projection, tree: TypeTree): U = {
      val o = full(basisPrism[Expr, U])
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

    import example._

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
        val o = full(basisPrism[Expr, U])
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
    import state._


    def apply[F[_]: Monad: MonadState[?[_], InterpretationState]: MonoidK, U: Basis[Expr, ?]]
        : Interpreter[F, U, ScalarStage.Mask] = Interpreter[F, U, ScalarStage.Mask] { case ScalarStage.Mask(masks) =>
      MonadState[F, InterpretationState].get flatMap { state =>
        val o = full(basisPrism[Expr, U])
        val eraseId = Map("_id" -> o.int(0))
        def projectionObject(tree: TypeTree): Option[Map[String, U]] = {
          val initialConfig = Config(state.uniqueKey, missing[Expr, U](state.uniqueKey), state.mapper)
          val rebuilt = rebuildDoc[scala.Function1[Config[U], ?], U](Projection(List()), tree).apply(initialConfig)
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

  object Cartesian {
    import example._
    import Projection.Step._
    import state._
    import optics0._

    def apply[F[_]: Monad: MonadState[?[_], InterpretationState]: MonoidK, U: Basis[Expr, ?]](
        inner: Interpreter[F, U, ScalarStage])
        : Interpreter[F, U, ScalarStage.Cartesian] = new Interpreter[F, U, ScalarStage.Cartesian] {
      def apply(s: ScalarStage.Cartesian) = optToAlternative[F].apply(Projection.safeCartouches(s.cartouches)) flatMap {
        cartouches =>
          MonadState[F, InterpretationState].get flatMap { state =>
            val undefinedKey = state.uniqueKey.concat("_cartesian_empty")
            if (cartouches.isEmpty)
              unfocus[F] as List(Pipeline.Match(o.obj(Map(undefinedKey -> o.bool(false)))))
            else {
              val ns = x => state.uniqueKey.concat(x)
              val interpretations: F[List[List[Pipeline[U]]]] =
                cartouches.toList.traverse {
                  case (alias, (_, instructions)) => for {
                    _ <- MonadState[F, InterpretationState].set(InterpretationState(ns(alias.name), Mapper.Focus(ns(alias.name))))
                    res <- instructions foldMapM { x => for {
                      a <- inner(x)
                      _ <- focus[F]
                    } yield a }
                  } yield res
                }
              interpretations map { is =>
                val defaultObject = cartouches map {
                  case (alias, _) => ns(alias.name) -> o.str("$".concat(ns(alias.name)))
                }
                val initialProjection: Pipeline[U] =
                  Pipeline.Project(
                    Map("_id" -> o.int(0)) ++ (cartouches.map {
                      case (alias, (field, instructions)) =>
                        ns(alias.name) -> o.projection(state.mapper.inProjection(Projection.key(field.name)))
                    }))
                val instructions = is.flatMap(_.flatMap( {
                  case Pipeline.Project(mp) =>
                    List(Pipeline.Project(defaultObject ++ mp))
                  case Pipeline.Presented =>
                    List()
                  case Pipeline.Erase =>
                    List()
                  case x =>
                    List(x)
                }))
                val removeEmptyFields = Pipeline.Project { cartouches map {
                  case (alias, _) => alias.name -> o.cond(
                    o.eqx(List(
                      o.str("$".concat(ns(alias.name))),
                      missing[Expr, U](ns(alias.name)))),
                    missingKey[Expr, U](ns(alias.name)),
                    o.str("$".concat(ns(alias.name))))
                }}
                val removeEmptyObjects =
                  Pipeline.Match(o.or(cartouches.toList map {
                    case (k, v) => o.obj(Map(k.name -> o.exists(o.bool(true))))
                  }))
                List(initialProjection) ++ instructions ++ List(removeEmptyFields, removeEmptyObjects)
              } flatMap { pipes =>
                unfocus[F] as pipes
              }
            }
          }
      }
    }
  }
  object Pivot {
    import example._
    import state._
    import iota._
    def apply[F[_]: Monad: MonadState[?[_], InterpretationState], U: Basis[Expr, ?]]: Interpreter[F, U, ScalarStage.Pivot] =
      new Interpreter[F, U, ScalarStage.Pivot] {
        def apply(s: ScalarStage.Pivot): F[List[Pipeline[U]]] = s match { case ScalarStage.Pivot(status, vectorType) =>
          for {
            state <- MonadState[F, InterpretationState].get
            res = mkPipes(status, vectorType, state)
            _ <- focus[F]
          } yield res.map(_.map(mapProjection[Expr, U, U](state.mapper.inProjection(_))))
        }
        def ensureArray(vectorType: ColumnType.Vector, key: String, undefined: U): Pipeline[U] ={
          val proj = o.projection(Projection(List()))
          Pipeline.Project(Map(key -> (vectorType match {
            case ColumnType.Object =>
              o.cond(
                o.or(List(o.not(o.eqx(List(o.typ(proj), o.str("object")))), o.eqx(List(proj, o.obj(Map()))))),
                o.array(List(o.obj(Map("k" -> undefined, "v" -> undefined)))),
                o.objectToArray(o.projection(Projection(List()))))
            case ColumnType.Array =>
              o.cond(
                o.or(List(o.not(o.eqx(List(o.typ(proj), o.str("array")))), o.eqx(List(proj, o.array(List()))))),
                o.array(List(undefined)),
                proj)
          })))
        }

        def mkValue(status: IdStatus, vectorType: ColumnType.Vector, unwinded: String, index: String, undefined: U): U = {
          val indexString = o.str("$".concat(index))
          val unwindString = o.str("$".concat(unwinded))
          val kString = o.str("$".concat(unwinded).concat(".k"))
          val vString = o.str("$".concat(unwinded).concat(".v"))
          vectorType match {
            case ColumnType.Array => status match {
              case IdStatus.IdOnly =>
                o.cond(
                  o.eqx(List(unwindString, undefined)),
                  undefined,
                  indexString)
              case IdStatus.ExcludeId =>
                unwindString
              case IdStatus.IncludeId =>
                o.cond(
                  o.eqx(List(unwindString, undefined)),
                  undefined,
                  o.array(List(indexString, unwindString)))
            }
            case ColumnType.Object => status match {
              case IdStatus.IdOnly => kString
              case IdStatus.ExcludeId => vString
              case IdStatus.IncludeId =>
                o.cond(
                  o.eqx(List(vString, undefined)),
                  undefined,
                  o.array(List(kString, vString)))
            }
          }
        }
        def mkPipes(status: IdStatus, vectorType: ColumnType.Vector, state: InterpretationState): List[Pipeline[U]] = {
          val unwindKey = state.uniqueKey.concat("_unwind")
          val indexKey = state.uniqueKey.concat("_unwind_index")
          List(
            ensureArray(vectorType, unwindKey, missing[Expr, U](state.uniqueKey)),
            Pipeline.Unwind(unwindKey, indexKey),
            Pipeline.Project(Map(
              "_id" -> o.int(0),
              state.uniqueKey -> mkValue(status, vectorType, unwindKey, indexKey, missing[Expr, U](state.uniqueKey)))),
            Pipeline.Presented)
        }
      }
  }

  object Project {
    import optics0._
    import Projection._
    import Projection.Step._
    import example._
    import state._
    def apply[F[_]: Monad: MonadState[?[_], InterpretationState]: MonoidK, U: Basis[Expr, ?]]: Interpreter[F, U, ScalarStage.Project] =
      new Interpreter[F, U, ScalarStage.Project] {
        def apply(s: ScalarStage.Project): F[List[Pipeline[U]]] =
          optToAlternative[F].apply(Projection.fromCPath(s.path)) flatMap { (prj: Projection) =>
            MonadState[F, InterpretationState].get map { state =>
              val o = full(basisPrism[Expr, U])
              val tmpKey0 = state.uniqueKey.concat("_project0")
              val tmpKey1 = state.uniqueKey.concat("_project1")
              val fld = state.mapper.inProjection(prj)
              val initialProjection = Pipeline.Project(Map(tmpKey0 -> o.projection(Projection(List()))))
              initialProjection :: build(state.uniqueKey, fld, Field(tmpKey0), Field(tmpKey1), Field(state.uniqueKey), List())
            } flatMap { a => focus[F] as a }
          }

        def stepType: Step => U = {
          case Step.Field(_) => o.str("object")
          case Step.Index(_) => o.str("array")
        }

        @scala.annotation.tailrec
        def build(key: String, prj: Projection, input: Field, output: Field, res: Field, acc: List[Pipeline[U]])
            : List[Pipeline[U]] = {
          prj.steps match {
            case List() =>
              acc ++ List(
                Pipeline.Project(Map(res.keyPart -> o.projection(Projection(List(input))))),
                Pipeline.Presented)
            case hd :: tail =>
              val projectionObject =
                o.cond(
                  o.or(List(
                    o.not(o.eqx(List(o.typ(o.projection(Projection(List(input)))), stepType(hd)))),
                    o.eqx(List(o.typ(o.projection(Projection(List(input, hd)))), o.str("missing"))))),
                  missing[Expr, U](key),
                  o.projection(Projection(List(input, hd))))
              val project: Pipeline[U] = Pipeline.Project(Map(output.keyPart -> projectionObject))
              build(key, Projection(tail), output, input, res, acc ++ List(project, Pipeline.Presented))
          }
        }
      }
  }
  object Wrap {
    import optics0._
    import Projection._
    import Projection.Step._
    import example._
    import state._
    import iota._

    def apply[F[_]: Monad: MonadState[?[_], InterpretationState]: MonoidK, U: Basis[Expr, ?]]
        : Interpreter[F, U, ScalarStage.Wrap] =
      new Interpreter[F, U, ScalarStage.Wrap] {
        def apply(s: ScalarStage.Wrap): F[List[Pipeline[U]]] = optToAlternative[F].apply(Projection.safeField(s.name)) flatMap { name =>
          for {
            state <- MonadState[F, InterpretationState].get
            (res: List[Pipeline[U]]) = List(Pipeline.Project(Map(
              state.uniqueKey ->
                o.cond(
                  o.eqx(List(o.projection(Projection(List())), missing[Expr, U](state.uniqueKey))),
                  missing[Expr, U](state.uniqueKey),
                  o.obj(Map(name.name -> o.projection(Projection(List()))))),
              "_id" -> o.int(0))))
            _ <- focus[F]
          } yield res.map(_.map(mapProjection[Expr, U, U](state.mapper.inProjection(_))))
        }
      }
  }

  import state._

  def apply[F[_]: Monad: MonadState[?[_], InterpretationState]: MonoidK, U: Basis[Expr, ?]](
      pushdown: PushdownLevel)
      : Interpreter[F, U, ScalarStage] =
    Interpreter[F, U, ScalarStage] {  s =>
      if (pushdown < PushdownLevel.Light) MonoidK[F].empty[List[Pipeline[U]]]
      else s match {
        case s: ScalarStage.Wrap => Wrap[F, U].apply(s)
        case s: ScalarStage.Mask => Mask[F, U].apply(s)
        case s: ScalarStage.Pivot => Pivot[F, U].apply(s)
        case s: ScalarStage.Project => Project[F, U].apply(s)
        case s: ScalarStage.Cartesian if pushdown < PushdownLevel.Full => MonoidK[F].empty[List[Pipeline[U]]]
        case s: ScalarStage.Cartesian => Cartesian[F, U](apply[F, U](pushdown)).apply(s)
      }
    }

  final case class Interpretation(stages: List[ScalarStage], docs: List[BsonDocument])

  def interpretIdStatus[F[_]: Applicative: MonadState[?[_], InterpretationState], U: Basis[Expr, ?]](
      uq: String)
      : Interpreter[F, U, IdStatus] =
    new Interpreter[F, U, IdStatus] {
      def apply(s: IdStatus): F[List[Pipeline[U]]] = s match {
        case IdStatus.IdOnly =>
          focus[F] as List(Pipeline.Project(Map(
            uq -> o.str("$_id"),
            "_id" -> o.int(0))))
        case IdStatus.ExcludeId =>
          List[Pipeline[U]]().pure[F]
        case IdStatus.IncludeId =>
          focus[F] as List(Pipeline.Project(Map(
            uq -> o.array(List(o.key("$_id"), o.projection(Projection(List())))),
            "_id" -> o.int(0))))
      }
    }

  def interpretStages(version: Version, pushdown: PushdownLevel, uuid: String, stages: ScalarStages): (Interpretation, Mapper) = {
    type InState[A] = StateT[Option, InterpretationState, A]
    val stage = apply[InState, Fix[Expr]](pushdown)
    val idStatus = interpretIdStatus[InState, Fix[Expr]](uuid)
    def refine(inp: Interpretation): InState[Either[Interpretation, Interpretation]] = inp.stages match {
      case List() => inp.asRight[Interpretation].pure[InState]
      case hd :: tail =>
        val nextStep = for {
          pipes <- stage.apply(hd)
          docs <- compilePipeline[InState, Fix[Expr]](version, uuid, pipes)
        } yield Interpretation(tail, inp.docs ++ docs).asLeft[Interpretation]
        nextStep <+> inp.asRight.pure[InState]
      }
    val interpreted = for {
      initPipes <- idStatus(stages.idStatus)
      initDocs <- compilePipeline[InState, Fix[Expr]](version, uuid, initPipes)
      refined <- Monad[InState].tailRecM(Interpretation(stages.stages, initDocs))(refine)
    } yield refined
    interpreted.run(InterpretationState(uuid, Mapper.Unfocus)) match {
      case None =>
        (Interpretation(stages.stages, List()), Mapper.Unfocus)
      case Some((state, a)) =>
        (a, state.mapper)
    }
  }

    import algebras._
    import iota._

    def coreToBson[U: Basis[Core, ?]]: U => BsonValue = {
      import scala.collection.JavaConverters._
      scheme.cata[Core, U, BsonValue](Algebra {
        case Core.Null() =>
          new BsonNull()
        case Core.SInt(i) =>
          new BsonInt32(i)
        case Core.SString(s) =>
          new BsonString(s)
        case Core.Bool(b) =>
          new BsonBoolean(b)
        case Core.Array(as) =>
          new BsonArray(as.asJava)
        case Core.Object(mp) =>
          val elems = mp.toList.map {
            case (k, v) => new BsonElement(k, v)
          }
          new BsonDocument(elems.asJava)
      })
    }

    def compileProjections[U: Basis[Expr, ?], W: Basis[CoreOp, ?]](uuid: String): U => W =
      scheme.cata[Expr, U, W](Algebra { x =>
        CopK.RemoveL[Const[Projection, ?], ExprL].apply(x) match {
          case Left(a) => compute(a).embed
          case Right(Const(prj)) => unfoldProjection[W](uuid).apply(prj)
        }
      })

    def onlyDocuments[F[_]: MonoidK: Applicative](inp: BsonValue): F[BsonDocument] = inp match {
      case x: BsonDocument => x.pure[F]
      case _ => MonoidK[F].empty
    }

    def compilePipeline[F[_]: MonoidK: Monad, U: Basis[Expr, ?]](
        version: Version,
        uuid: String,
        pipes: List[Pipeline[U]])
        : F[List[BsonDocument]] =
      pipes.flatMap(eraseCustomPipeline(uuid, _)).traverse { (pipe: MongoPipeline[U]) =>
        if (version < pipe.minVersion) MonoidK[F].empty
        else {
          val exprs = pipelineObjects[Expr, U](pipe)
          val coreop: Fix[CoreOp] = compileProjections[U, Fix[CoreOp]](uuid).apply(exprs)
          val optimized = (letOpt[Fix[CoreOp], Fix[CoreOp]] andThen orOpt[Fix[CoreOp], Fix[CoreOp]])(coreop)
          val coreopMinVersion = coreOpsVersionRun[Fix[CoreOp]].apply(optimized)
          if (coreopMinVersion > version) MonoidK[F].empty
          else {
            val core0: Fix[Core0] = coreOpsToCore0Run[Fix[CoreOp], Fix[Core0]].apply(optimized)
            val core: Fix[Core] = core0ToCore[Fix[Core0], Fix[Core]].apply(core0)
            val bsonValue = coreToBson[Fix[Core]].apply(core)
            onlyDocuments[F](bsonValue)
          }
        }
      }


}
