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

import quasar.common.{CPath, CPathField, CPathIndex}

import _root_.iota.TListK.:::
import _root_.iota.{CopK, TListK, TNilK}
import _root_.iota.syntax._

import cats._
import cats.data.Const
import cats.implicits._
import higherkindness.droste._
import higherkindness.droste.data._
import higherkindness.droste.syntax.all._
import higherkindness.droste.syntax.compose._
import higherkindness.droste.util.DefaultTraverse

import monocle.{Iso, Prism}
import monocle.macros.{GenLens, GenPrism}

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
    val basePrism: Prism[O, Core[A]]

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

    val array = basePrism composePrism _array
    val obj = basePrism composePrism _obj
    val bool = basePrism composePrism _bool
    val int = basePrism composePrism _int
    val str = basePrism composePrism _str
    val nil = basePrism composePrism _nil
  }
  def optics[A, O](bp: Prism[O, Core[A]]) = new Optics[A, O] { val basePrism = bp }
}

sealed trait Op[A] extends Product with Serializable

object Op {
  final case class Let[A](vars: Map[String, A], in: A) extends Op[A]
  final case class Type[A](exp: A) extends Op[A]
  final case class Eq[A](exps: List[A]) extends Op[A]
  final case class Or[A](exps: List[A]) extends Op[A]
  final case class Exists[A](exp: A) extends Op[A]
  final case class Cond[A](check: A, ok: A, fail: A) extends Op[A]
  final case class Ne[A](exp: A) extends Op[A]
  final case class ObjectToArray[A](exp: A) extends Op[A]
  final case class ArrayElemAt[A](exp: A, ix: Int) extends Op[A]
  final case class Reduce[A](input: A, initialValue: A, expression: A) extends Op[A]
  final case class ConcatArrays[A](exps: List[A]) extends Op[A]
  final case class Not[A](exp: A) extends Op[A]

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
    val basePrism: Prism[O, Op[A]]

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

    val let = basePrism composePrism _let
    val typ = basePrism composePrism _type
    val eq = basePrism composePrism _eq
    val or = basePrism composePrism _or
    val exists = basePrism composePrism _exists
    val cond = basePrism composePrism _cond
    val ne = basePrism composePrism _ne
    val objectToArray = basePrism composePrism _objectToArray
    val arrayElemAt = basePrism composePrism _arrayElemAt
    val reduce = basePrism composePrism _reduce
    val concatArrays = basePrism composePrism _concatArrays
    val not = basePrism composePrism _not
  }

  def optics[A, O](bp: Prism[O, Op[A]]): Optics[A, O] = new Optics[A, O] { val basePrism = bp }
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

  trait Grouped extends Product with Serializable

  object Groupped {
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
    val basePrism: Prism[O, Const[Projection, A]]

    val _projection =
      Iso[Const[Projection, A], Projection](_.getConst)(Const(_))
    val projection =
      basePrism composePrism _projection.asPrism
  }

  def optics[A, O](bp: Prism[O, Const[Projection, A]]): Optics[A, O] = new Optics[A, O] { val basePrism = bp }
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
}

object example {
  import iota._
  import Core._
  import Op._
  import optics0._
  def thisNil[F[a] <: ACopK[a], U](implicit I: Core :<<: F, P: Basis[F, U]): U = {
    val optics = Core.optics(basisPrism[F, U] composePrism copkPrism[Core, F, U])
    import optics._
    val a: U = ???
    a match {
      case nil(_) => "O"
      case array(lst) => "lst"
    }
    nil()
  }


  type CoreOp[A] = CopK[Core ::: Op ::: TNilK, A]
  type Core0[A] = CopK[Core ::: TNilK, A]

  def nilCoreOp: Fix[CoreOp] = thisNil[CoreOp, Fix[CoreOp]]
  def nilCore: Mu[Core0] = thisNil[Core0, Mu[Core0]]
//  def nilCoattr: Coattr[Core0, Int] = thisNil[CoattrF[Core0, Int, ?], Coattr[Core0, Int]]

  def opsToCore[U: Basis[Core0, ?]]: Algebra[Op, U] = {
    val core0 = Core.optics(basisPrism[Core0, U] composePrism copkPrism[Core, Core0, U])
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

  def coreOpsToCoreRun[W: Basis[CoreOp, ?], U: Basis[Core0, ?]]: W => U =
    scheme.cata[CoreOp, W, U](coreOpsToCore)

  type Elem = (Projection.Grouped, Int)
  def unfoldProjection: CVCoalgebra[CoreOp, List[Elem]] = {
    type U = Coattr[CoreOp, List[Elem]]
    type F[X] = CoattrF[CoreOp, List[Elem], X]
    val core = Core.optics(basisPrism[F, U] composePrism coattrFPrism[CoreOp, List[Elem], U] composePrism copkPrism[Core, CoreOp, U])
    val op = Op.optics(basisPrism[F, U] composePrism coattrFPrism[CoreOp, List[Elem], U] composePrism copkPrism[Op, CoreOp, U])
    import core._
    import op._

    val cf = Core.optics(copkPrism[Core, CoreOp, U])
    val of = Core.optics(copkPrism[Op, CoreOp, U])
    CVCoalgebra {
      case List() => cf.str("$$ROOT")
    }
  }
}
