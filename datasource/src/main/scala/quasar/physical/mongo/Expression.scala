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

import slamdata.Predef.{String => SString, Int => SInt, _}

import monocle.{Iso, Prism}

import matryoshka._
import matryoshka.implicits._

import monocle._

import org.bson._

import quasar.{RenderTree, NonTerminal, Terminal}, RenderTree.ops._

import scalaz.{Applicative, Traverse, Scalaz, Const, Coproduct}
import Scalaz._

trait Core[A] extends Product with Serializable
trait Op[A] extends Product with Serializable
trait Projected[A] extends Product with Serializable

object Core {
  final case class Array[A](arr: List[A]) extends Core[A]
  final case class Object[A](obj: Map[SString, A]) extends Core[A]
  final case class Bool[A](bool: Boolean) extends Core[A]
  final case class Int[A](int: SInt) extends Core[A]
  final case class String[A](string: SString) extends Core[A]
  final case class Null[A]() extends Core[A]

  implicit def traverse: Traverse[Core] = new Traverse[Core] {
    def traverseImpl[G[_]: Applicative, A, B](core: Core[A])(f: A => G[B])
        : G[Core[B]] = core match {

      case Null() => (Null(): Core[B]).point[G]
      case String(s) => (String(s): Core[B]).point[G]
      case Int(i) => (Int(i): Core[B]).point[G]
      case Bool(b) => (Bool(b): Core[B]).point[G]
      case Object(o) => o traverse f map (Object(_))
      case Array(a) => a traverse f map (Array(_))
    }
  }

  trait Optics[A, O] {
    val corePrism: Prism[O, Core[A]]
    val _array: Prism[Core[A], List[A]] =
      Prism.partial[Core[A], List[A]] {
        case Array(a) => a
      } { a => Array(a) }

    val _obj: Prism[Core[A], Map[SString, A]] =
      Prism.partial[Core[A], Map[SString, A]] {
        case Object(a) => a
      } { a => Object(a) }

    val _bool: Prism[Core[A], Boolean] =
      Prism.partial[Core[A], Boolean] {
        case Bool(a) => a
      } { a => Bool(a) }

    val _int: Prism[Core[A], SInt] =
      Prism.partial[Core[A], SInt] {
        case Int(a) => a
      } { a => Int(a) }

    val _string: Prism[Core[A], SString] =
      Prism.partial[Core[A], SString] {
        case String(a) => a
      } { a => String(a) }

    val _nil: Prism[Core[A], Unit] =
      Prism.partial[Core[A], Unit] {
        case Null() => ()
      } { x => Null() }

    val array: Prism[O, List[A]] = corePrism composePrism _array
    val obj: Prism[O, Map[SString, A]] = corePrism composePrism _obj
    val bool: Prism[O, Boolean] = corePrism composePrism _bool
    val int: Prism[O, SInt] = corePrism composePrism _int
    val string: Prism[O, SString] = corePrism composePrism _string
    val nil: Prism[O, Unit] = corePrism composePrism _nil
  }

  implicit def renderTree[A: RenderTree]: RenderTree[Core[A]] = RenderTree.make {
    case Array(a) => NonTerminal(List("Array"), None, a map (_.render))
    case Object(a) => NonTerminal(List("Object"), None, a.toList map {
      case (k, v) => NonTerminal(List(), Some(k), List(v.render)).render
    })
    case Bool(a) => Terminal(List("Boolean"), Some(a.toString))
    case Int(a) => Terminal(List("Int"), Some(a.toString))
    case String(a) => Terminal(List("String"), Some(a))
    case Null() => Terminal(List("Null"), None)
  }
}

object Op {
  final case class Let[A](vars: Map[SString, A], in: A) extends Op[A]
  final case class Type[A](exp: A) extends Op[A]
  final case class Eq[A](exps: List[A]) extends Op[A]
  final case class Or[A](exps: List[A]) extends Op[A]
  final case class Exists[A](exp: A) extends Op[A]
  final case class Cond[A](check: A, ok: A, fail: A) extends Op[A]
  final case class Ne[A](exp: A) extends Op[A]
  final case class ObjectToArray[A](exp: A) extends Op[A]
  final case class ArrayElemAt[A](exp: A, ix: SInt) extends Op[A]

  implicit def traverse: Traverse[Op] = new Traverse[Op] {
    def traverseImpl[G[_]: Applicative, A, B](op: Op[A])(f: A => G[B])
        : G[Op[B]] = op match {

      case ArrayElemAt(x, i) => f(x) map (ArrayElemAt(_, i))
      case ObjectToArray(x) => f(x) map (ObjectToArray(_))
      case Ne(x) => f(x) map (Ne(_))
      case Exists(x) => f(x) map (Exists(_))
      case Cond(a, b, c) => (f(a) |@| f(b) |@| f(c))(Cond(_, _, _))
      case Or(lst) => lst traverse f map (Or(_))
      case Eq(lst) => lst traverse f map (Eq(_))
      case Type(x) => f(x) map (Type(_))
      case Let(vars, in) => ((vars traverse f) |@| f(in))(Let(_, _))
    }
  }
  implicit def renderTree[A: RenderTree]: RenderTree[Op[A]] = RenderTree.make {
    case Let(vars, in) => NonTerminal(List("Let"), None, List(
      NonTerminal(List(), Some("vars"), vars.toList map {
        case (k, v) => NonTerminal(List(), Some(k), List(v.render))
      }),
      NonTerminal(List(), Some("in"), List(in.render))) map (_.render))
    case Type(a) => NonTerminal(List("Type"), None, List(a.render))
    case Eq(a) => NonTerminal(List("Eq"), None, a map (_.render))
    case Or(a) => NonTerminal(List("Or"), None, a map (_.render))
    case Exists(a) => NonTerminal(List("Exists"), None, List(a.render))
    case Cond(a, b, c) => NonTerminal(List("Cond"), None, List(
      NonTerminal(List(), Some("check"), List(a.render)).render,
      NonTerminal(List(), Some("ok"), List(b.render)).render,
      NonTerminal(List(), Some("fail"), List(c.render)).render
    ))
    case Ne(a) => NonTerminal(List("Ne"), None, List(a.render))
    case ObjectToArray(a) => NonTerminal(List("ObjectToArray"), None, List(a.render))
    case ArrayElemAt(a, i) => NonTerminal(List("ArrayElemAt"), Some(i.toString), List(a.render))
  }

  trait Optics[A, O] {
    val opPrism: Prism[O, Op[A]]
    val _let: Prism[Op[A], (Map[SString, A], A)] =
      Prism.partial[Op[A], (Map[SString, A], A)] {
        case Let(vars, in) => (vars, in)
      } { case (vars, in) => Let(vars, in) }
    val _type: Prism[Op[A], A] =
      Prism.partial[Op[A], A] {
        case Type(a) => a
      } { a => Type(a) }
    val _eq: Prism[Op[A], List[A]] =
      Prism.partial[Op[A], List[A]] {
        case Eq(a) => a
      } { a => Eq(a) }
    val _or: Prism[Op[A], List[A]] =
      Prism.partial[Op[A], List[A]] {
        case Or(a) => a
      } { a => Or(a) }
    val _exists: Prism[Op[A], A] =
      Prism.partial[Op[A], A] {
        case Exists(a) => a
      } { a => Exists(a) }
    val _cond: Prism[Op[A], (A, A, A)] =
      Prism.partial[Op[A], (A, A, A)] {
        case Cond(a, b, c) => (a, b, c)
      } { case (a, b, c) => Cond(a, b, c) }
    val _arrayElemAt: Prism[Op[A], (A, SInt)] =
      Prism.partial[Op[A], (A, SInt)] {
        case ArrayElemAt(a, i) => (a, i)
      } { case (a, i) => ArrayElemAt(a, i) }
    val _objectToArray: Prism[Op[A], A] =
      Prism.partial[Op[A], A] {
        case ObjectToArray(a) => a
      } { a => ObjectToArray(a) }
    val _ne: Prism[Op[A], A] =
      Prism.partial[Op[A], A] {
        case Ne(a) => a
      } { a => Ne(a) }

    val $let: Prism[O, (Map[SString, A], A)] = opPrism composePrism _let
    val $type: Prism[O, A] = opPrism composePrism _type
    val $eq: Prism[O, List[A]] = opPrism composePrism _eq
    val $or: Prism[O, List[A]] = opPrism composePrism _or
    val $exists: Prism[O, A] = opPrism composePrism _exists
    val $cond: Prism[O, (A, A, A)] = opPrism composePrism _cond
    val $arrayElemAt: Prism[O, (A, SInt)] = opPrism composePrism _arrayElemAt
    val $objectToArray: Prism[O, A] = opPrism composePrism _objectToArray
    val $ne: Prism[O, A] = opPrism composePrism _ne
  }
}

trait Step extends Product with Serializable

final case class Field(f: SString) extends Step
final case class Index(i: SInt) extends Step

final case class Projection(steps: List[Step]) extends Product with Serializable

object Step {
  implicit val renderTree: RenderTree[Step] = RenderTree.make {
    case Field(s) => Terminal(List("Field"), Some(s))
    case Index(i) => Terminal(List("Index"), Some(i.toString))
  }
}

object Projection {
  implicit val renderTree: RenderTree[Projection] = RenderTree.make { x =>
    NonTerminal(List("Projection"), None, x.steps map (_.render))
  }
}

object Optics {
  class CoreOptics[A, O](val corePrism: Prism[O, Core[A]])
      extends Core.Optics[A, O]

  class CoreOpOptics[A, O](val corePrism: Prism[O, Core[A]], val opPrism: Prism[O, Op[A]])
      extends Core.Optics[A, O]
      with Op.Optics[A, O]

  class FullOptics[A, O](
      val corePrism: Prism[O, Core[A]],
      val opPrism: Prism[O, Op[A]],
      val prjPrism: Prism[O, Const[Projection, A]])
      extends Core.Optics[A, O]
      with Op.Optics[A, O] {

    val _projection: Iso[Const[Projection, A], List[Step]] =
      Iso[Const[Projection, A], List[Step]] {
        case Const(Projection(steps)) => steps
      } { steps => Const(Projection(steps)) }

    val projection: Prism[O, List[Step]] =
      prjPrism composeIso _projection
  }
}

trait Pipeline[+A] extends Product with Serializable
object Pipeline {
  final case class $project[A](obj: Map[SString, A]) extends Pipeline[A]
  final case class $match[A](obj: Map[SString, A]) extends Pipeline[A]
  final case class $unwind(path: SString, arrayIndex: SString) extends Pipeline[Nothing]
}

trait CustomPipeline extends Pipeline[Nothing]
object CustomPipeline {
  final case object Erase extends CustomPipeline
  final case class NotNull(field: SString) extends CustomPipeline
}

object Expression {
  import CustomPipeline._
  import Pipeline._
  import Op._
  import Core._

  type CoreOp[A] = Coproduct[Op, Core, A]
  type Projected[A] = Coproduct[Const[Projection, ?], CoreOp, A]

  import scalaz.Inject, Inject._

  @scala.annotation.tailrec
  def pipelineObjects[T[_[_]]: BirecursiveT](pipeline: Pipeline[T[Projected]]): T[Projected] = {
    def prism[A]: Prism[Projected[A], Core[A]] =
      Prism((x: Projected[A]) => Inject[Core, Projected].prj(x))((x: Core[A]) => Inject[Core, Projected].inj(x))
    val corePrism =
      birecursiveIso[T[Projected], Projected].reverse composePrism prism[T[Projected]]

    val O = new Optics.CoreOptics(corePrism)

    pipeline match {
      case Erase =>
        pipelineObjects($match(Map("non_existent_field" -> O.array(List()))))
      case NotNull(fld) =>
        pipelineObjects($match(Map("non_existent_field" -> O.bool(true))))
      case $match(mp) =>
        O.obj(Map("$match" -> O.obj(mp)))
      case $project(mp) =>
        O.obj(Map("$project" -> O.obj(mp)))
      case $unwind(path, arrayIndex) =>
        O.obj(Map("$unwind" -> O.obj(Map(
          "path" -> O.string(path),
          "includeArrayIndex" -> O.string(arrayIndex),
          "preserveNullAndEmptyArrays" -> O.bool(true)))))
    }
  }

  def unfoldProjection[T[_[_]]: BirecursiveT](prj: Projection): T[CoreOp] = {
    def coreInj[A] =
      Prism((x: CoreOp[A]) => Inject[Core, CoreOp].prj(x))((x: Core[A]) => Inject[Core, CoreOp].inj(x))
    def opInj[A] =
      Prism((x: CoreOp[A]) => Inject[Op, CoreOp].prj(x))((x: Op[A]) => Inject[Op, CoreOp].inj(x))

    trait GrouppedSteps
    final case class IndexGroup(i: SInt) extends GrouppedSteps
    final case class FieldGroup(s: List[SString]) extends GrouppedSteps
    final case class IndexedAccess(i: SInt) extends GrouppedSteps

    def groupSteps(prj: Projection): List[GrouppedSteps] = {
      val accum = prj.steps.foldl ((List[SString](), List[GrouppedSteps]())) {
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

    type Elem = (GrouppedSteps, SInt)

    val O = new Optics.CoreOpOptics[List[Elem], CoreOp[List[Elem]]](coreInj, opInj)

    val ψ: Coalgebra[CoreOp, List[Elem]] = {
      case List() =>
        O.string("$$ROOT")
      case (FieldGroup(hd :: tail), _) :: List() =>
        O.string(tail.foldl(hd) { accum => s => accum.concat(".").concat(s) })
      case (hd, levelIx) :: tl => hd match {
        case IndexedAccess(i) =>
          O.$arrayElemAt(tl, i)
        case IndexGroup(i) =>
          val level = "level".concat(levelIx.toString)
          val varSteps: List[Elem] = (IndexedAccess(i), levelIx) :: tl
          val vars: Map[SString, List[Elem]] = Map(level -> varSteps)
          val expSteps: GrouppedSteps = FieldGroup(List(level))
          val exp = List((expSteps, 0))
          O.$let(vars, exp)
        case FieldGroup(steps) =>
          val level = "level".concat(levelIx.toString)
          val vars = Map(level -> tl)
          val expSteps: GrouppedSteps = FieldGroup(level :: steps)
          val exp = List((expSteps, 0))
          O.$let(vars, exp)
      }
    }
    groupSteps(prj).zipWithIndex.reverse.ana[T[CoreOp]](ψ)
  }

  def compileProjections[T[_[_]]: BirecursiveT](inp: T[Projected]): T[CoreOp] = {
    def τ(inp: Const[Projection, T[CoreOp]]): CoreOp[T[CoreOp]] =
      unfoldProjection(inp.getConst).project
    inp.transCata[T[CoreOp]](_.run.fold(τ, (x => x)))
  }

  def compileOps[T[_[_]]: BirecursiveT](inp: T[CoreOp]): T[Core] = {
    val O = new Optics.CoreOptics(birecursiveIso[T[Core], Core].reverse.asPrism)
    def τ(inp: Op[T[Core]]): Core[T[Core]] = inp match {
      case Let(vars, in) => O._obj(Map("$let" -> O.obj(Map(
        "vars" -> O.obj(vars),
        "in" -> in))))
      case Type(a) => O._obj(Map("$type"-> a))
      case Eq(a) => O._obj(Map("$eq" -> O.array(a)))
      case Or(a) => O._obj(Map("$or" -> O.array(a)))
      case Exists(a) => O._obj(Map("$exists" -> a))
      case Cond(a, b, c) => O._obj(Map("$cond" -> O.array(List(a, b, c))))
      case Ne(a) => O._obj(Map("$ne" -> a))
      case ObjectToArray(a) => O._obj(Map("$objectToArray" -> a))
      case ArrayElemAt(a, ix) => O._obj(Map("$arrayElemAt" -> O.array(List(a, O.int(ix)))))
    }

    inp.transCata[T[Core]](_.run.fold(τ, (x => x)))
  }

  def optimizeOrs[T[_[_]]: BirecursiveT](inp: T[CoreOp]): T[CoreOp] = {
    val iso =
      birecursiveIso[T[CoreOp], CoreOp].reverse
    val coreInj =
      Prism((x: CoreOp[T[CoreOp]]) => Inject[Core, CoreOp].prj(x))((x: Core[T[CoreOp]]) => Inject[Core, CoreOp].inj(x))
    val opInj =
      Prism((x: CoreOp[T[CoreOp]]) => Inject[Op, CoreOp].prj(x))((x: Op[T[CoreOp]]) => Inject[Op, CoreOp].inj(x))

    val O = new Optics.CoreOpOptics(
      iso composePrism coreInj,
      iso composePrism opInj)

    def τ(inp: CoreOp[T[CoreOp]]): CoreOp[T[CoreOp]] = inp match {
      case opInj(Or(lst)) => lst match {
        case a :: List() => a.project
        case List() => O.bool(false).project
        case x => inp
      }
      case x => x
    }
    inp.transCata[T[CoreOp]](τ)
  }

  def coreToBson[T[_[_]]: BirecursiveT](inp: T[Core]): BsonValue = {
    import scala.collection.JavaConverters._

    def ϕ: Algebra[Core, BsonValue] = {
      case Null() =>
        new BsonNull()
      case Int(i) =>
        new BsonInt32(i)
      case String(s) =>
        new BsonString(s)
      case Bool(b) =>
        new BsonBoolean(b)
      case Array(as) =>
        new BsonArray(as.asJava)
      case Object(mp) =>
        val elems = mp.toList map {
          case (key, v) => new BsonElement(key, v)
        }
        new BsonDocument(elems.asJava)
    }
    inp.cata[BsonValue](ϕ)
  }
}
