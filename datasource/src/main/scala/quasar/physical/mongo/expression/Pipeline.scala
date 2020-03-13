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

trait Pipeline[+A] extends Product with Serializable

trait MongoPipeline[+A] extends Pipeline[A]

trait CustomPipeline extends Pipeline[Nothing]

import scalaz.Functor

object Pipeline {
  final case class $project[A](obj: Map[String, A]) extends MongoPipeline[A]
  final case class $match[A](a: A) extends MongoPipeline[A]
  final case class $unwind[A](path: String, arrayIndex: String) extends MongoPipeline[A]

  final case object Presented extends CustomPipeline
  final case object Erase extends CustomPipeline

  implicit val delayRenderTreeMongoPipeline: Delay[RenderTree, Pipeline] =
    new Delay[RenderTree, Pipeline] {
      def apply[A](fa: RenderTree[A]): RenderTree[Pipeline[A]] = RenderTree.make {
        case $project(obj) => NonTerminal(List("$project"), None, obj.toList map {
          case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
        })
        case $match(a) => NonTerminal(List("$match"), None, List(fa.render(a)))
        case $unwind(p, a) =>
          NonTerminal(List("$unwind"), None, List(p.render, a.render))
        case Presented =>
          Terminal(List("Presented"), None)
        case Erase =>
          Terminal(List("Erase"), None)
      }
  }

  def pipeMinVersion(pipe: MongoPipeline[_]): Version = pipe match {
    case $project(_) => Version.zero
    case $match(_) => Version.zero
    case $unwind(_, _) => Version.$unwind
  }

  implicit val functorPipeline: Functor[Pipeline] = new Functor[Pipeline] {
    def map[A, B](fa: Pipeline[A])(f: A => B): Pipeline[B] = fa match {
      case Presented => Presented
      case Erase => Erase
      case $project(obj) => $project(obj map { case (k, v) => (k, f(v)) })
      case $match(a) => $match(f(a))
      case $unwind(p, i) => $unwind(p, i)
    }
  }
}
