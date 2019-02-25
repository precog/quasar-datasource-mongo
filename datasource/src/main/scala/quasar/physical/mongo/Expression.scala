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

import quasar.common.{CPath, CPathField, CPathIndex}
import quasar.{RenderTree, NonTerminal, Terminal, RenderTreeT}, RenderTree.ops._
import quasar.fp.PrismNT

import scalaz.{Equal, Order, Ordering, Applicative, Traverse, Scalaz, Const, Coproduct, :<:}
import Scalaz._
import scalaz.syntax._
import scalaz.std._

trait Core[A] extends Product with Serializable
trait Op[A] extends Product with Serializable

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

  implicit val delayRenderTreeCore: Delay[RenderTree, Core] = new Delay[RenderTree, Core] {
    def apply[A](fa: RenderTree[A]): RenderTree[Core[A]] = RenderTree.make {
      case Array(a) => NonTerminal(List("Array"), None, a map fa.render)
        case Object(a) => NonTerminal(List("Object"), None, a.toList map {
          case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
        })
      case Bool(a) => Terminal(List("Boolean"), Some(a.toString))
      case Int(a) => Terminal(List("Int"), Some(a.toString))
      case String(a) => Terminal(List("String"), Some(a))
      case Null() => Terminal(List("Null"), None)
    }

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

  implicit val delayRenderTreeOp: Delay[RenderTree, Op] = new Delay[RenderTree, Op] {
    def apply[A](fa: RenderTree[A]): RenderTree[Op[A]] = RenderTree.make {
      case Let(vars, in) => NonTerminal(List("Let"), None, List(
        NonTerminal(List(), Some("vars"), vars.toList map {
          case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
        }),
        NonTerminal(List(), Some("in"), List(fa.render(in)))))
      case Type(a) => NonTerminal(List("Type"), None, List(fa.render(a)))
      case Eq(a) => NonTerminal(List("Eq"), None, a map fa.render)
      case Or(a) => NonTerminal(List("Or"), None, a map fa.render)
      case Exists(a) => NonTerminal(List("Exists"), None, List(fa.render(a)))
      case Cond(a, b, c) => NonTerminal(List("Cond"), None, List(
        NonTerminal(List(), Some("check"), List(fa.render(a))),
        NonTerminal(List(), Some("ok"), List(fa.render(b))),
        NonTerminal(List(), Some("fail"), List(fa.render(c)))
      ))
      case Ne(a) => NonTerminal(List("Ne"), None, List(fa.render(a)))
      case ObjectToArray(a) => NonTerminal(List("ObjectToArray"), None, List(fa.render(a)))
      case ArrayElemAt(a, i) => NonTerminal(List("ArrayElemAt"), Some(i.toString), List(fa.render(a)))
    }
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

final case class Projection(steps: List[Step]) extends Product with Serializable {
  private def keyPart(step: Step): SString = step match {
    case Field(s) => s
    case Index(i) => i.toString
  }

  def +(prj: Projection): Projection = Projection(steps ++ prj.steps)
  def toKey: SString = steps match {
    case List() => "$$ROOT"
    case hd :: tail => tail.foldl(keyPart(hd)) { acc => x => acc.concat(".").concat(keyPart(x)) }
  }
}
final case class Field(name: SString) extends Step
final case class Index(ix: SInt) extends Step

object Step {

  implicit val renderTreeStep: RenderTree[Step] = RenderTree.make {
    case Field(s) => Terminal(List("Field"), Some(s))
    case Index(i) => Terminal(List("Index"), Some(i.toString))
  }

  implicit val orderStep: Order[Step] = new Order[Step] {
    def order(a: Step, b: Step) = a match {
      case Field(x) => b match {
        case Field(y) => Order[SString].order(x, y)
        case Index(_) => Ordering.LT
      }
      case Index(x) => b match {
        case Index(y) => Order[SInt].order(x, y)
        case Field(_) => Ordering.GT
      }
    }
  }
}

object Projection {
  implicit val renderTreeProjection: RenderTree[Projection] = RenderTree.make { x =>
    NonTerminal(List("Projection"), None, x.steps map (_.render))
  }

  implicit val equal: Equal[Projection] = new Equal[Projection] {
    def equal(a: Projection, b: Projection): Boolean = a.steps === b.steps
  }

  def key(s: SString): Projection = Projection(List(Field(s)))
  def index(i: SInt): Projection = Projection(List(Index(i)))

  implicit val orderProjection: Order[Projection] = new Order[Projection] {
    def order(a: Projection, b: Projection): Ordering =
      Order[List[Step]].order(a.steps.toList, b.steps.toList)
  }

  def safeField(str: SString): Option[Field] =
    if (str contains ".") None
    else Some(Field(str))

  def safeCartouches[A](inp: Map[CPathField, (CPathField, A)]): Option[Map[Field, (Field, A)]] =
    inp.toList.foldlM(Map.empty[Field, (Field, A)]){ acc => {
      case (alias, (focus, lst)) => for {
        newAlias <- safeField(alias.name)
        newFocus <- safeField(focus.name)
      } yield acc.updated(newAlias, (newFocus, lst))
    }}

  def fromCPath(cpath: CPath): Option[Projection] =
    cpath.nodes.foldMapM {
      case CPathField(str) => safeField(str) map (List(_))
      case CPathIndex(ix) => Some(List(Index(ix)))
      case _ => None
    } map (Projection(_))
}

object Optics {
  class CoreOptics[A, O, F[_]] private[Optics] (basePrism: Prism[O, F[A]])(implicit c: Core :<: F) extends {
    val corePrism = basePrism composePrism PrismNT.inject[Core, F].asPrism[A]
  } with Core.Optics[A, O]

  class CoreOpOptics[A, O, F[_]] private[Optics] (basePrism: Prism[O, F[A]])(
      implicit c: Core :<: F, o: Op :<: F) extends {
    val corePrism = basePrism composePrism PrismNT.inject[Core, F].asPrism[A]
    val opPrism = basePrism composePrism PrismNT.inject[Op, F].asPrism[A]
  } with Core.Optics[A, O] with Op.Optics[A, O]

  class FullOptics[A, O, F[_]] private[Optics] (basePrism: Prism[O, F[A]])(
      implicit c: Core :<: F, o: Op :<: F, p: Const[Projection, ?] :<: F) extends {
    val corePrism = basePrism composePrism PrismNT.inject[Core, F].asPrism[A]
    val opPrism = basePrism composePrism PrismNT.inject[Op, F].asPrism[A]
    val prjPrism = basePrism composePrism PrismNT.inject[Const[Projection, ?], F].asPrism[A]
  }  with Core.Optics[A, O] with Op.Optics[A, O] {
    val _steps: Iso[Const[Projection, A], List[Step]] =
      Iso[Const[Projection, A], List[Step]] {
        case Const(Projection(steps)) => steps
      } { steps => Const(Projection(steps)) }

    val steps: Prism[O, List[Step]] =
      prjPrism composeIso _steps

    val _projection: Iso[Const[Projection, A], Projection] =
      Iso[Const[Projection, A], Projection] {
        case Const(p) => p
      } { p => Const(p) }

    val projection: Prism[O, Projection] =
      prjPrism composeIso _projection

    val key: Prism[O, SString] =
      steps composePrism {
        Prism.partial[List[Step], SString] {
          case Field(s) :: List() => s
        } { s => List(Field(s)) }
      }
  }

  def core[A, O, F[_]](basePrism: Prism[O, F[A]])(implicit c: Core :<: F): CoreOptics[A, O, F] =
    new CoreOptics(basePrism)

  def coreOp[A, O, F[_]](basePrism: Prism[O, F[A]])(implicit c: Core :<: F, o: Op :<: F): CoreOpOptics[A, O, F] =
    new CoreOpOptics(basePrism)

  def full[A, O, F[_]](basePrism: Prism[O, F[A]])(
      implicit c: Core :<: F,
      o: Op :<: F,
      p: Const[Projection, ?] :<: F)
      : FullOptics[A, O, F] = {
    new FullOptics(basePrism)
  }
}

trait Pipeline[+A] extends Product with Serializable

trait MongoPipeline[+A] extends Pipeline[A]
object Pipeline {
  final case class $project[A](obj: Map[SString, A]) extends MongoPipeline[A]
  final case class $match[A](obj: Map[SString, A]) extends MongoPipeline[A]
  final case class $unwind[A](path: SString, arrayIndex: SString) extends MongoPipeline[A]

  implicit val delayRenderTreeMongoPipeline: Delay[RenderTree, MongoPipeline] = new Delay[RenderTree, MongoPipeline] {
    def apply[A](fa: RenderTree[A]): RenderTree[MongoPipeline[A]] = RenderTree.make {
      case $project(obj) => NonTerminal(List("$project"), None, obj.toList map {
        case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
      })
      case $match(obj) => NonTerminal(List("$match"), None, obj.toList map {
        case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
      })
      case $unwind(p, a) => NonTerminal(List("$unwind"), None, List(p.render, a.render))
    }
  }
}

trait CustomPipeline extends Pipeline[Nothing]
object CustomPipeline {
  final case class NotNull(field: SString) extends CustomPipeline
}

object Expression {
  import CustomPipeline._
  import Pipeline._
  import Op._
  import Core._
  type CoreOp[A] = Coproduct[Op, Core, A]
  type Projected[A] = Coproduct[Const[Projection, ?], CoreOp, A]

  def pipeMinVersion[A](pipe: MongoPipeline[A]): Version = pipe match {
    case $project(_) => Version.zero
    case $match(_) => Version.zero
    case $unwind(_, _) => Version.$unwind
  }

  def opVersion[A](op: Op[A]): Version = op match {
    case Let(_, _) => Version.zero
    case Type(_) => Version.$type
    case Eq(_) => Version.zero
    case Or(_) => Version.zero
    case Exists(_) => Version.zero
    case Cond(_, _, _) => Version.zero
    case ArrayElemAt(_, _) => Version.$arrayElemAt
    case ObjectToArray(_) => Version.$objectToArray
    case Ne(_) => Version.zero
  }

  def compilePipeline[T[_[_]]: BirecursiveT](
      version: Version,
      pipes: List[Pipeline[T[Projected]]])
      : Option[List[BsonDocument]] = {

    pipes.foldMapM { x => compilePipe(version, x) map (List(_)) }
  }

  def compilePipe[T[_[_]]: BirecursiveT](version: Version, customPipe: Pipeline[T[Projected]]): Option[BsonDocument] = {
    val pipe = eraseCustomPipeline(customPipe)
    if (version < pipeMinVersion(pipe)) None
    else {
      def transformM(op: Op[T[Core]]): Option[Core[T[Core]]] = {
        if (version < opVersion(op)) None
        else Some(opsToCore[T](op))
      }

      optimizeOrs(compileProjections(pipelineObjects(pipe)))
        .transCataM[Option, T[Core], Core] (_.run.fold(transformM, Some(_)))
        .map(coreToBson[T](_))
        .flatMap(mbBsonDocument)
    }
  }
  val mbBsonDocument: BsonValue => Option[BsonDocument] = {
    case x: BsonDocument => Some(x)
    case _ => None
  }

  def eraseCustomPipeline[T[_[_]]: BirecursiveT](pipeline: Pipeline[T[Projected]]): MongoPipeline[T[Projected]] = {
    val O = Optics.full(birecursiveIso[T[Projected], Projected].reverse.asPrism)

    pipeline match {
      case NotNull(fld) => $match(Map(fld -> O.$ne(O.nil())))
      case $project(obj) => $project(obj)
      case $match(obj) => $match(obj)
      case $unwind(a, i) => $unwind(a, i)
    }
  }

  def pipelineObjects[T[_[_]]: BirecursiveT](pipeline: MongoPipeline[T[Projected]]): T[Projected] = {
    val O = Optics.full(birecursiveIso[T[Projected], Projected].reverse.asPrism)
    pipeline match {
      case $match(mp) =>
        O.obj(Map("$match" -> O.obj(mp)))
      case $project(mp) =>
        O.obj(Map("$project" -> O.obj(mp)))
      case $unwind(path, arrayIndex) =>
        O.obj(Map("$unwind" -> O.obj(Map(
          "path" -> O.key(path),
          "includeArrayIndex" -> O.string(arrayIndex),
          "preserveNullAndEmptyArrays" -> O.bool(true)))))
    }
  }

  def unfoldProjection[T[_[_]]: BirecursiveT](prj: Projection): T[CoreOp] = {
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

    val O = Optics.coreOp(Prism.id[CoreOp[List[Elem]]])

    val ψ: Coalgebra[CoreOp, List[Elem]] = {
      case List() =>
        O.string("$$ROOT")
      case (FieldGroup(hd :: tail), _) :: List() =>
        O.string(tail.foldl("$" concat hd) { accum => s => accum concat "." concat s  })
      case (hd, levelIx) :: tl => hd match {
        case IndexedAccess(i) =>
          O.$arrayElemAt(tl, i)
        case IndexGroup(i) =>
          val level = "level" concat levelIx.toString
          val varSteps: List[Elem] = (IndexedAccess(i), levelIx) :: tl

          val vars: Map[SString, List[Elem]] = Map(level -> tl)
//          val expSteps: GrouppedSteps = FieldGroup(List("$$" concat level))
          val expSteps = IndexedAccess(i)
          val exp = List((expSteps, 0), (FieldGroup(List("$" concat level)), 1))
//          val exp = List() //(IndexedAccess(i),
          O.$let(vars, exp)
        case FieldGroup(steps) =>
          val level = "level".concat(levelIx.toString)
          val vars = Map(level -> tl)
          val expSteps: GrouppedSteps = FieldGroup("$".concat(level) :: steps)
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

  def opsToCore[T[_[_]]: BirecursiveT](inp: Op[T[Core]]): Core[T[Core]] = {
    val O = Optics.core(birecursiveIso[T[Core], Core].reverse.asPrism)
    inp match {
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
  }

  def compileOps[T[_[_]]: BirecursiveT](inp: T[CoreOp]): T[Core] = {
    inp.transCata[T[Core]](_.run.fold(opsToCore[T], (x => x)))
  }

  def optimizeOrs[T[_[_]]: BirecursiveT](inp: T[CoreOp]): T[CoreOp] = {
    def opPrismNT = PrismNT.inject[Op, CoreOp]

    val O = Optics.coreOp(birecursiveIso[T[CoreOp], CoreOp].reverse.asPrism)

    val opList = opPrismNT.asPrism[List[T[CoreOp]]]

    val ϕ: Algebra[CoreOp, List[T[CoreOp]]] = {
      case opList(Or(lst)) => lst flatMap (x => x)
      case x =>
        // erase children, bubble coreop flatten list of list of functors embed functors into t
        List(x map (a => List[T[CoreOp]]())) map (_.sequence) flatMap (x => x) map (_.embed)
    }

    val opT = opPrismNT.asPrism[T[CoreOp]]
    def τ(inp: CoreOp[T[CoreOp]]): CoreOp[T[CoreOp]] = inp match {
      case opT(Or(lst)) => lst match {
        case a :: List() => a.project
        case List() => O.bool(false).project
        case prjs => O.$or(O.$or(prjs).cata[List[T[CoreOp]]](ϕ)).project
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
