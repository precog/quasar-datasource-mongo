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

import quasar.common.{CPath, CPathField, CPathIndex}
import quasar.{RenderTree, NonTerminal, Terminal}, RenderTree.ops._

import scalaz.{Order, Ordering, Scalaz}, Scalaz._

object Projection {
  trait Step extends Product with Serializable {
    def keyPart: String
  }

  object Step {
    final case class Field(name: String) extends Step {
      def keyPart: String = name
    }

    final case class Index(ix: Int) extends Step {
      def keyPart: String = ix.toString
    }

    implicit val renderTreeStep: RenderTree[Step] = RenderTree.make {
      case Field(s) => Terminal(List("Field"), Some(s))
      case Index(i) => Terminal(List("Index"), Some(i.toString))
    }

    implicit val orderStep: Order[Step] = new Order[Step] {
      def order(a: Step, b: Step) = a match {
        case Field(x) => b match {
          case Field(y) => Order[String].order(x, y)
          case Index(_) => Ordering.LT
        }
        case Index(x) => b match {
          case Index(y) => Order[Int].order(x, y)
          case Field(_) => Ordering.GT
        }
      }
    }
  }

  import Step._

  implicit val renderTreeProjection: RenderTree[Projection] = RenderTree.make { x =>
    NonTerminal(List("Projection"), None, x.steps map (_.render))
  }

  def key(s: String): Projection = Projection(List(Field(s)))

  def index(i: Int): Projection = Projection(List(Index(i)))

  implicit val orderProjection: Order[Projection] = new Order[Projection] {
    def order(a: Projection, b: Projection): Ordering =
      Order[List[Step]].order(a.steps.toList, b.steps.toList)
  }

  def safeField(str: String): Option[Field] =
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

final case class Projection(steps: List[Projection.Step]) extends Product with Serializable {
  def +(prj: Projection): Projection = Projection(steps ++ prj.steps)
  def toKey: String = steps match {
    case List() => "$$ROOT"
    case hd :: tail => tail.foldl(hd.keyPart){ acc => x => acc concat "." concat x.keyPart }
  }
}
