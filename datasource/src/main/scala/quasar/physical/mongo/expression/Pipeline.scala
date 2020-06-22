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

import matryoshka.Delay

import quasar.{RenderTree, NonTerminal, Terminal}, RenderTree.ops._
import quasar.physical.mongo.Version

import cats.Functor

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

  implicit val delayRenderTreeMongoPipeline: Delay[RenderTree, Pipeline] =
    new Delay[RenderTree, Pipeline] {
      def apply[A](fa: RenderTree[A]): RenderTree[Pipeline[A]] = RenderTree.make {
        case Project(obj) => NonTerminal(List("$project"), None, obj.toList map {
          case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
        })
        case Match(a) => NonTerminal(List("$match"), None, List(fa.render(a)))
        case Unwind(p, a) =>
          NonTerminal(List("$unwind"), None, List(p.render, a.render))
        case Presented =>
          Terminal(List("Presented"), None)
        case Erase =>
          Terminal(List("Erase"), None)
      }
  }
}
