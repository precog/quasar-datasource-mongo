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

package quasar.physical.mongo.expression

import slamdata.Predef._

import matryoshka.Delay

import monocle.Prism

import quasar.{RenderTree, NonTerminal}
import quasar.physical.mongo.Version

import scalaz.{Traverse, Applicative, Scalaz}, Scalaz._
import scalaz.syntax._

trait Op[A] extends Product with Serializable

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
    val _let: Prism[Op[A], (Map[String, A], A)] =
      Prism.partial[Op[A], (Map[String, A], A)] {
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
    val _arrayElemAt: Prism[Op[A], (A, Int)] =
      Prism.partial[Op[A], (A, Int)] {
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

    val $let: Prism[O, (Map[String, A], A)] = opPrism composePrism _let
    val $type: Prism[O, A] = opPrism composePrism _type
    val $eq: Prism[O, List[A]] = opPrism composePrism _eq
    val $or: Prism[O, List[A]] = opPrism composePrism _or
    val $exists: Prism[O, A] = opPrism composePrism _exists
    val $cond: Prism[O, (A, A, A)] = opPrism composePrism _cond
    val $arrayElemAt: Prism[O, (A, Int)] = opPrism composePrism _arrayElemAt
    val $objectToArray: Prism[O, A] = opPrism composePrism _objectToArray
    val $ne: Prism[O, A] = opPrism composePrism _ne
  }

  def opMinVersion[A](op: Op[A]): Version = op match {
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
}
