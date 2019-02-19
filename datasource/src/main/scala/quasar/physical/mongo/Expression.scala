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

package quasar.physical.mongo

import slamdata.Predef._

import monocle.Prism

import iotaz.{CopK, TNilK}
import iotaz.TListK.:::

import matryoshka._
import matryoshka.implicits._
import matryoshka.data._

import monocle._

import quasar.contrib.iota._
import quasar.fp.PrismNT

import scalaz.{Applicative, Traverse, Scalaz, Const}
import Scalaz._

trait Core[T[_[_]], A] extends Product with Serializable
object Core {
  final case class Array[T[_[_]], A](arr: List[A]) extends Core[T, A]
  final case class Object[T[_[_]], A](obj: Map[String, A]) extends Core[T, A]
  final case class Bool[T[_[_]], A](bool: Boolean) extends Core[T, A]
  final case class Integer[T[_[_]], A](int: Int) extends Core[T, A]
  final case class Text[T[_[_]], A](text: String) extends Core[T, A]
  final case class Null[T[_[_]], A]() extends Core[T, A]

  implicit def traverse[T[_[_]]]: Traverse[Core[T, ?]] = new Traverse[Core[T, ?]] {
    def traverseImpl[G[_]: Applicative, A, B](core: Core[T, A])(f: A => G[B])
        : G[Core[T, B]] = core match {

      case Null() => (Null(): Core[T, B]).point[G]
      case Text(t) => (Text(t): Core[T, B]).point[G]
      case Integer(i) => (Integer(i): Core[T, B]).point[G]
      case Bool(b) => (Bool(b): Core[T, B]).point[G]
      case Object(o) => o traverse f map (Object(_))
      case Array(a) => a traverse f map (Array(_))
    }
  }
}

trait CoreDSL[T[_[_]], F[_], X] {
  def injCore(core: Core[T, X]): F[X]
  def prjCore(f: F[X]): Option[Core[T, X]]

  object array {
    def apply(arr: List[X]): F[X] =
      injCore(Core.Array(arr))

    def unapply(f: F[X]): Option[List[X]] = prjCore(f) match {
      case Some(Core.Array(a)) => Some(a)
      case _ => None
    }
  }
  object obj {
    def apply(obj: Map[String, X]): F[X] =
      injCore(Core.Object(obj))

    def unapply(f: F[X]): Option[Map[String, X]] = prjCore(f) match {
      case Some(Core.Object(a)) => Some(a)
      case _ => None
    }
  }
  object bool {
    def apply(b: Boolean): F[X] =
      injCore(Core.Bool(b))

    def unapply(f: F[X]): Option[Boolean] = prjCore(f) match {
      case Some(Core.Bool(b)) => Some(b)
      case _ => None
    }
  }
  object int {
    def apply(i: Int): F[X] =
      injCore(Core.Integer(i))

    def unapply(f: F[X]): Option[Int] = prjCore(f) match {
      case Some(Core.Integer(i)) => Some(i)
      case _ => None
    }
  }
  object string {
    def apply(s: String): F[X] =
      injCore(Core.Text(s))

    def unapply(f: F[X]): Option[String] = prjCore(f) match {
      case Some(Core.Text(s)) => Some(s)
      case _ => None
    }
  }
  object nil {
    def apply: F[X] =
      injCore(Core.Null())

    def unapply(f: F[X]): Option[Unit] = prjCore(f) match {
      case Some(Core.Null()) => Some(())
      case _ => None
    }
  }
}

trait Op[T[_[_]], A] extends Product with Serializable
object Op {
  final case class $let[T[_[_]], A](vars: Map[String, A], in: A) extends Op[T, A]
  final case class $type[T[_[_]], A](exp: A) extends Op[T, A]
  final case class $eq[T[_[_]], A](exps: List[A]) extends Op[T, A]
  final case class $or[T[_[_]], A](exps: List[A]) extends Op[T, A]
  final case class $cond[T[_[_]], A](check: A, ok: A, fail: A) extends Op[T, A]
  final case class $exists[T[_[_]], A](exp: A) extends Op[T, A]
  final case class $ne[T[_[_]], A](exp: A) extends Op[T, A]
  final case class $objectToArray[T[_[_]], A](exp: A) extends Op[T, A]
  final case class $arrayElemAt[T[_[_]], A](exp: A, ix: Int) extends Op[T, A]

  implicit def traverse[T[_[_]]]: Traverse[Op[T, ?]] = new Traverse[Op[T, ?]] {
    def traverseImpl[G[_]: Applicative, A, B](op: Op[T, A])(f: A => G[B])
        : G[Op[T, B]] = op match {

      case $arrayElemAt(x, i) => f(x) map ($arrayElemAt(_, i))
      case $objectToArray(x) => f(x) map ($objectToArray(_))
      case $ne(x) => f(x) map ($ne(_))
      case $exists(x) => f(x) map ($exists(_))
      case $cond(a, b, c) => (f(a) |@| f(b) |@| f(c))($cond(_, _, _))
      case $or(lst) => lst traverse f map ($or(_))
      case $eq(lst) => lst traverse f map ($eq(_))
      case $type(x) => f(x) map ($type(_))
      case $let(vars, in) => ((vars traverse f) |@| f(in))($let(_, _))
    }
  }

}

trait OpDSL[T[_[_]], F[_], X] {
  def injOp(op: Op[T, X]): F[X]
  def prjOp(f: F[X]): Option[Op[T, X]]
  object $let {
    def apply(vars: Map[String, X], in: X): F[X] =
      injOp(Op.$let(vars, in))

    def unapply(f: F[X]): Option[(Map[String, X], X)] = prjOp(f) match {
      case Some(Op.$let(vars, in)) => Some((vars, in))
      case _ => None
    }
  }
  object $type {
    def apply(expr: X): F[X] =
      injOp(Op.$type(expr))

    def unapply(f: F[X]): Option[X] = prjOp(f) match {
      case Some(Op.$type(expr)) => Some(expr)
      case _ => None
    }
  }
  object $eq {
    def apply(exprs: List[X]): F[X] =
      injOp(Op.$eq(exprs))

    def unapply(f: F[X]): Option[List[X]] = prjOp(f) match {
      case Some(Op.$eq(exprs)) => Some(exprs)
      case _ => None
    }
  }
  object $or {
    def apply(exprs: List[X]): F[X] =
      injOp(Op.$or(exprs))

    def unapply(f: F[X]): Option[List[X]] = prjOp(f) match {
      case Some(Op.$or(exprs)) => Some(exprs)
      case _ => None
    }
  }
  object $cond {
    def apply(check: X, ok: X, fail: X): F[X] =
      injOp(Op.$cond(check, ok, fail))

    def unapply(f: F[X]): Option[(X, X, X)] = prjOp(f) match {
      case Some(Op.$cond(check, ok, fail)) => Some((check, ok, fail))
      case _ => None
    }
  }
  object $exists {
    def apply(expr: X): F[X] =
      injOp(Op.$exists(expr))

    def unapply(f: F[X]): Option[X] = prjOp(f) match {
      case Some(Op.$exists(expr)) => Some(expr)
      case _ => None
    }
  }
  object $ne {
    def apply(expr: X): F[X] =
      injOp(Op.$ne(expr))

    def unapply(f: F[X]): Option[X] = prjOp(f) match {
      case Some(Op.$ne(expr)) => Some(expr)
      case _ => None
    }
  }
  object $objectToArray {
    def apply(expr: X): F[X] =
      injOp(Op.$objectToArray(expr))

    def unapply(f: F[X]): Option[X] = prjOp(f) match {
      case Some(Op.$objectToArray(expr)) => Some(expr)
      case _ => None
    }
  }
  object $arrayElemAt {
    def apply(expr: X, ix: Int): F[X] =
      injOp(Op.$arrayElemAt(expr, ix))

    def unapply(f: F[X]): Option[(X, Int)] = prjOp(f) match {
      case Some(Op.$arrayElemAt(expr, ix)) => Some((expr, ix))
      case _ => None
    }
  }
}

trait ProjectionStep extends Product with Serializable
final case class Field(f: String) extends ProjectionStep
final case class Index(i: Int) extends ProjectionStep

final case class Projection(steps: List[ProjectionStep]) extends Product with Serializable

trait ProjectionDSL[F[_]] {
  def injProjection[A](prj: Const[Projection, A]): F[A]
  def prjProjection[A](f: F[A]): Option[Const[Projection, A]]
  object projection {
    def apply[A](prj: Projection): F[A] =
      injProjection(Const(prj))

    def unapply[A](f: F[A]): Option[Projection] = prjProjection(f) match {
      case Some(Const(proj)) => Some(proj)
      case _ => None
    }
  }
}

trait Pipeline[+A] extends Product with Serializable
object Pipeline {
  final case class $project[A](obj: Map[String, A]) extends Pipeline[A]
  final case class $match[A](obj: Map[String, A]) extends Pipeline[A]
  final case class $unwind(path: String, arrayIndex: String) extends Pipeline[Nothing]
}

trait CustomPipeline extends Pipeline[Nothing]
object CustomPipeline {
  final case object Erase extends CustomPipeline
  final case class NotNull(field: String) extends CustomPipeline
}

object Expression {
  import CustomPipeline._
  import Pipeline._


  type CoreOpList[T[_[_]]] =
    Op[T, ?] ::: Core[T, ?] ::: TNilK
  type CoreOp[T[_[_]], A] =
    CopK[CoreOpList[T], A]

  type ProjectedList[T[_[_]]] =
    Const[Projection, ?] ::: CoreOpList[T]
  type Projected[T[_[_]], A] =
    CopK[ProjectedList[T], A]

  import iotaz.TListK, TListK._
  def compute[INP <: TListK, OUT <: TListK, A](
      inp: CopK[INP, A])(
      implicit ev: Compute.Aux[INP, OUT])
      : CopK[OUT, A] =
    inp.asInstanceOf[CopK[ev.Out, A]]


  def pipelineObjects[T[_[_]]: BirecursiveT](
      pipeline: Pipeline[T[Projected[T, ?]]])
      : T[Projected[T, ?]] = {

    type F[A] = Projected[T, A]
    type R = T[F]
    val C = new CoreDSL[T, F, R]{
      def injCore(c: Core[T, R]): F[R] = CopK.Inject[Core[T, ?], F].inj(c)
      def prjCore(f: F[R]): Option[Core[T, R]] = CopK.Inject[Core[T, ?], F].prj(f)
    }

    pipeline match {
      case Erase =>
        pipelineObjects($match(Map("non-existent-field" -> C.array(List()).embed)))
      case NotNull(fld) =>
        ???
//        pipelineObjects($match(Map(fld -> O.$ne[R, F].reverseGet(C.nil))))
      case $match(mp) =>
        C.obj(mp).embed
      case $project(mp) =>
        C.obj(mp).embed
      case $unwind(path, indexFld) =>
        C.obj(Map(
          "path" -> C.string(path).embed,
          "includeArrayIndex" -> C.string(indexFld).embed,
          "preserveNullAndEmptyArrays" -> C.bool(true).embed
        )).embed
    }
  }

  def unfoldProjection[T[_[_]]: BirecursiveT](prj: Projection): T[CoreOp[T, ?]] = {
    type F[A] = CoreOp[T, A]
    type R = T[F]
    val O = new AnyRef with CoreDSL[T, F, List[Elem]] with OpDSL[T, F, List[Elem]] {
      def injCore(a: Core[T, List[Elem]]): F[List[Elem]] = CopK.Inject[Core[T, ?], F].inj(a)
      def prjCore(a: F[List[Elem]]): Option[Core[T, List[Elem]]] = CopK.Inject[Core[T, ?], F].prj(a)
      def injOp(a: Op[T, List[Elem]]): F[List[Elem]] = CopK.Inject[Op[T, ?], F].inj(a)
      def prjOp(a: F[List[Elem]]): Option[Op[T, List[Elem]]] = CopK.Inject[Op[T, ?], F].prj(a)
    }

    trait GrouppedSteps
    final case class IndexGroup(i: Int) extends GrouppedSteps
    final case class FieldGroup(s: List[String]) extends GrouppedSteps
    final case class IndexedAccess(i: Int) extends GrouppedSteps

    def groupSteps(prj: Projection): List[GrouppedSteps] = {
      val accum = prj.steps.foldl ((List[String](), List[GrouppedSteps]())) {
        case (fldAccum, accum) => {
          case Field(s) => (s :: fldAccum, accum)
          case Index(i) => (List(), IndexGroup(i) :: FieldGroup(fldAccum.reverse) :: accum)
        }
      }
      accum._1 match {
        case List() => accum._2.reverse
        case x => (FieldGroup(x.reverse) :: accum._2).reverse
      }
    }

    type Elem = (GrouppedSteps, Int)

    val ψ: Coalgebra[F, List[Elem]] = {
      case List() =>
        O.string("$$ROOT")
      case (IndexedAccess(i), _) :: tail =>
        O.$arrayElemAt(tail, i)
      case (FieldGroup(hd :: tail), _) :: List() =>
        val str = tail.foldl(hd) { accum => s => accum.concat(".").concat(s) }
        O.string(str)
      case (IndexGroup(i), levelIx) :: tl =>
        val level = "level".concat(levelIx.toString)
        val varSteps: List[Elem] = (IndexedAccess(i), levelIx) :: tl
        val vars: Map[String, List[Elem]] = Map(level -> varSteps)
        val expSteps: GrouppedSteps = FieldGroup(List(level))
        val exp = List((expSteps, 0))
        O.$let(vars, exp)
      case (FieldGroup(steps), levelIx) :: tail =>
        val level = "level".concat(levelIx.toString)
        val vars = Map(level -> tail)
        val expSteps: GrouppedSteps = FieldGroup(level :: steps)
        val exp = List((expSteps, 0))
        O.$ne(tail)
    }

    groupSteps(prj).zipWithIndex.reverse.ana[R](ψ)
  }

  def compileProjections[T[_[_]]: BirecursiveT](inp: T[Projected[T, ?]]): T[CoreOp[T, ?]] = {
    type F[A] = Projected[T, A]
    type G[A] = CoreOp[T, A]
    val removeL = CopK.RemoveL[Const[Projection, ?], ProjectedList[T]]
    val ψ: Transform[T[G], F, G] = x => removeL(x) match {
      case Left(a) => compute(a)
      case Right(Const(prj)) => unfoldProjection[T](prj).project
    }
    inp.transCata[T[G]](ψ)
  }

  def compileOps[T[_[_]]: BirecursiveT](inp: T[CoreOp[T, ?]]): T[Core[T, ?]] = {
    type F[A] = CoreOp[T, A]
    type G[A] = Core[T, A]
    val removeL = CopK.RemoveL[Op[T, ?], CoreOpList[T]]
    val ψ: Transform[T[G], F, G] = x => removeL(x) match {
      case Left(a) =>
        CopK.Inject[Core[T, ?], CopK[Core[T, ?] ::: TNilK, ?]].prj(compute(a)).getOrElse(Core.Null())
      case Right(f) => ???
    }
    inp.transCata[T[G]](ψ)
  }
}
