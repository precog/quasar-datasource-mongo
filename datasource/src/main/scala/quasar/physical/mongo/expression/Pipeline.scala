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

import cats.Bifunctor

import matryoshka.Delay

import quasar.{RenderTree, NonTerminal}, RenderTree.ops._
import quasar.physical.mongo.Version

trait Pipeline[+A, +B] extends Product with Serializable

trait MongoPipeline[+A, +B] extends Pipeline[A, B]

trait CustomPipeline extends Pipeline[Nothing, Nothing]

object Pipeline {
  final case class $project[A, B](obj: Map[String, A]) extends MongoPipeline[A, B]
  final case class $match[A, B](obj: Map[String, A]) extends MongoPipeline[A, B]
  final case class $unwind[A, B](path: B, arrayIndex: String) extends MongoPipeline[A, B]

  implicit def delayRenderTreeMongoPipeline[B: RenderTree]: Delay[RenderTree, MongoPipeline[?, B]] =
    new Delay[RenderTree, MongoPipeline[?, B]] {
      def apply[A](fa: RenderTree[A]): RenderTree[MongoPipeline[A, B]] = RenderTree.make {
        case $project(obj) => NonTerminal(List("$project"), None, obj.toList map {
          case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
        })
        case $match(obj) => NonTerminal(List("$match"), None, obj.toList map {
          case (k, v) => NonTerminal(List(), Some(k), List(fa.render(v)))
        })
        case $unwind(p, a) =>
          NonTerminal(List("$unwind"), None, List(p.render, a.render))
      }
  }

  final case class NotNull(field: String) extends CustomPipeline

  def pipeMinVersion(pipe: MongoPipeline[_, _]): Version = pipe match {
    case $project(_) => Version.zero
    case $match(_) => Version.zero
    case $unwind(_, _) => Version.$unwind
  }

  implicit val bifunctorMongoPipeline: Bifunctor[Pipeline] = new Bifunctor[Pipeline] {
    def bimap[A, B, C, D](fab: Pipeline[A, B])(f: A => C, g: B => D): Pipeline[C, D] = fab match {
      case $project(mp) => $project(mp map { case (a, b) => (a, f(b)) })
      case $match(mp) => $match(mp map { case (a, b) => (a, f(b)) })
      case $unwind(prj, arrayIndex) => $unwind(g(prj), arrayIndex)
      case NotNull(fld) => NotNull(fld)
    }
  }
}
