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

package quasar.physical.mongo.expression

import slamdata.Predef._

import quasar.{RenderTree, NonTerminal}
import quasar.physical.mongo.Version

import cats._
import cats.implicits._
import higherkindness.droste.Delay
import higherkindness.droste.util.DefaultTraverse
import monocle.Prism

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
  final case class Gte[A](exp: A) extends Op[A]
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
      case Gte(x) => f(x).map(Gte(_))
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
      Prism.partial[Op[A], (Map[String, A], A)]{ case Let(a, b) => (a, b) } { case (a, b) => Let(a, b) }
    val _type =
      Prism.partial[Op[A], A]{ case Type(v) => v } (Type(_))
    val _eq =
      Prism.partial[Op[A], List[A]]{ case Eq(v) => v } (Eq(_))
    val _or =
      Prism.partial[Op[A], List[A]]{ case Or(v) => v } (Or(_))
    val _exists =
      Prism.partial[Op[A], A]{ case Exists(v) => v } (Exists(_))
    val _cond =
      Prism.partial[Op[A], (A, A, A)]{ case Cond(a, b, c) => (a, b, c) } { case (a, b, c) => Cond(a, b, c) }
    val _ne =
      Prism.partial[Op[A], A]{ case Ne(v) => v } (Ne(_))
    val _gte =
      Prism.partial[Op[A], A]{ case Gte(v) => v } (Gte(_))
    val _objectToArray =
      Prism.partial[Op[A], A]{ case ObjectToArray(v) => v } (ObjectToArray(_))
    val _arrayElemAt =
      Prism.partial[Op[A], (A, Int)]{ case ArrayElemAt(a, b) => (a, b) } { case (a, b) => ArrayElemAt(a, b) }
    val _reduce =
      Prism.partial[Op[A], (A, A, A)]{ case Reduce(a, b, c) => (a, b, c) } { case (a, b, c) => Reduce(a, b, c) }
    val _concatArrays =
      Prism.partial[Op[A], List[A]]{ case ConcatArrays(v) => v } (ConcatArrays(_))
    val _not =
      Prism.partial[Op[A], A]{ case Not(v) => v } (Not(_))

    val let = op composePrism _let
    val typ = op composePrism _type
    val eqx = op composePrism _eq
    val or = op composePrism _or
    val exists = op composePrism _exists
    val cond = op composePrism _cond
    val nex = op composePrism _ne
    val gte = op composePrism _gte
    val objectToArray = op composePrism _objectToArray
    val arrayElemAt = op composePrism _arrayElemAt
    val reduce = op composePrism _reduce
    val concatArrays = op composePrism _concatArrays
    val not = op composePrism _not
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
        NonTerminal(List(), Some("fail"), List(fa.render(c)))))
      case Ne(a) => NonTerminal(List("Ne"), None, List(fa.render(a)))
      case Gte(a) => NonTerminal(List("Gte"), None, List(fa.render(a)))
      case ObjectToArray(a) => NonTerminal(List("ObjectToArray"), None, List(fa.render(a)))
      case ArrayElemAt(a, i) => NonTerminal(List("ArrayElemAt"), Some(i.toString), List(fa.render(a)))
      case Reduce(a, b, c) => NonTerminal(List("Reduce"), None, List(
        NonTerminal(List(), Some("input"), List(fa.render(a))),
        NonTerminal(List(), Some("initialValue"), List(fa.render(b))),
        NonTerminal(List(), Some("expression"), List(fa.render(c)))))
      case ConcatArrays(a) => NonTerminal(List("ConcatArrays"), None, a map fa.render)
      case Not(a) => NonTerminal(List("Not"), None, List(fa.render(a)))
    }
  }

}
