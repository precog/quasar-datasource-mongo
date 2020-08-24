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

import slamdata.{Predef => s}, s.{Int => _, String => _, Eq => _, _}

import quasar.{RenderTree, NonTerminal, Terminal}

import java.time.OffsetDateTime

import cats._
import cats.implicits._
import higherkindness.droste.Delay
import higherkindness.droste.util.DefaultTraverse
import monocle.Prism

sealed trait Core[A] extends Product with Serializable

object Core {
  final case class Array[A](value: List[A]) extends Core[A]
  final case class Object[A](value: Map[s.String, A]) extends Core[A]
  final case class Bool[A](value: Boolean) extends Core[A]
  final case class Int[A](value: s.Int) extends Core[A]
  final case class Long[A](value: s.Long) extends Core[A]
  final case class Double[A](value: s.Double) extends Core[A]
  final case class String[A](value: s.String) extends Core[A]
  final case class DateTime[A](value: OffsetDateTime) extends Core[A]
  final case class Null[A]() extends Core[A]


  implicit def traverse: Traverse[Core] = new DefaultTraverse[Core] {
    def traverse[G[_]: Applicative, A, B](fa: Core[A])(f: A => G[B]): G[Core[B]] = fa match {
      case Array(v) => v.traverse(f).map(Array(_))
      case Object(v) => v.toList.traverse(_.traverse(f)).map(x => Object(x.toMap))
      case Bool(v) => (Bool(v): Core[B]).pure[G]
      case Int(v) => (Int(v): Core[B]).pure[G]
      case Long(v) => (Long(v): Core[B]).pure[G]
      case Double(v) => (Double(v): Core[B]).pure[G]
      case String(v) => (String(v): Core[B]).pure[G]
      case DateTime(v) => (DateTime(v): Core[B]).pure[G]
      case Null() => (Null(): Core[B]).pure[G]
    }
  }

  trait Optics[A, O] {
    val core: Prism[O, Core[A]]

    val _array =
      Prism.partial[Core[A], List[A]]{case Array(v) => v}(Array(_))
    val _obj =
      Prism.partial[Core[A], Map[s.String, A]]{case Object(v) => v}(Object(_))
    val _bool =
      Prism.partial[Core[A], Boolean]{case Bool(v) => v}(Bool(_))
    val _int =
      Prism.partial[Core[A], s.Int]{case Int(v) => v}(Int(_))
    val _long =
      Prism.partial[Core[A], s.Long]{case Long(v) => v}(Long(_))
    val _double =
      Prism.partial[Core[A], s.Double]{case Double(v) => v}(Double(_))
    val _str =
      Prism.partial[Core[A], s.String]{case String(v) => v}(String(_))
    val _dateTime =
      Prism.partial[Core[A], OffsetDateTime]{case DateTime(v) => v}(DateTime(_))
    val _nil =
      Prism.partial[Core[A], Unit]{case Null() => ()}(x => Null())

    val array = core composePrism _array
    val obj = core composePrism _obj
    val bool = core composePrism _bool
    val int = core composePrism _int
    val long = core composePrism _long
    val double = core composePrism _double
    val str = core composePrism _str
    val dateTime = core composePrism _dateTime
    val nil = core composePrism _nil
  }

  implicit val delayRenderTreeCore: Delay[RenderTree, Core] = new Delay[RenderTree, Core] {
    def apply[A](fa: RenderTree[A]): RenderTree[Core[A]] = RenderTree.make {
      case Array(a) => NonTerminal(List("Array"), None, a map fa.render)
      case Object(a) => NonTerminal(List("Object"), None, a.toList map {
        case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
      })
      case Bool(a) => Terminal(List("Boolean"), Some(a.toString))
      case Int(a) => Terminal(List("Int"), Some(a.toString))
      case Long(a) => Terminal(List("Long"), Some(a.toString))
      case Double(a) => Terminal(List("Double"), Some(a.toString))
      case String(a) => Terminal(List("String"), Some(a))
      case DateTime(a) => Terminal(List("DateTime"), Some(a.toString))
      case Null() => Terminal(List("Null"), None)
    }
  }
}
