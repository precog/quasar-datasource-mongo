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

import slamdata.Predef.{String => SString, Int => SInt, _}

import matryoshka.Delay

import monocle.Prism

import quasar.{RenderTree, NonTerminal, Terminal}

import scalaz.{Traverse, Applicative, Scalaz}, Scalaz._
import scalaz.syntax._

trait Core[A] extends Product with Serializable

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
