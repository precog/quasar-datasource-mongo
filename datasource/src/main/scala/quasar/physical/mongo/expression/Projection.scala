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

import quasar.common.{CPath, CPathField, CPathIndex}
import quasar.{RenderTree, NonTerminal, Terminal}, RenderTree.ops._

import cats._
import cats.data.Const
import cats.implicits._
import monocle.{Iso, Prism}

final case class Projection(steps: List[Projection.Step]) {
  def +(prj: Projection): Projection = Projection(steps ++ prj.steps)
  def toKey: String = steps match {
    case List() => "$$ROOT"
    case hd :: tail => tail.foldLeft(hd.keyPart){(acc, x) => s"$acc.${x.keyPart}"}
  }

  def toCPath: CPath = CPath(steps map {
    case Projection.Step.Field(s) => CPathField(s)
    case Projection.Step.Index(i) => CPathIndex(i)
  })
}

object Projection {
  sealed trait Step extends Product with Serializable {
    def keyPart: String
  }
  object Step {
    final case class Field(name: String) extends Step {
      def keyPart: String = name
    }
    final case class Index(ix: Int) extends Step {
      def keyPart: String = ix.toString
    }

    implicit val order: Order[Step] = new Order[Step] {
      def compare(a: Step, b: Step): Int = (a, b) match {
        case (Field(_), Index(_)) => -1
        case (Field(a), Field(b)) => Order[String].compare(a, b)
        case (Index(_), Field(_)) => 1
        case (Index(a), Index(b)) => Order[Int].compare(a, b)
        case (_, _) => -1
      }
    }

    implicit val renderTreeStep: RenderTree[Step] = RenderTree.make {
      case Field(s) => Terminal(List("Field"), Some(s))
      case Index(i) => Terminal(List("Index"), Some(i.toString))
    }
  }
  import Step._
  implicit val order: Order[Projection] = Order.by(_.steps)

  def key(s: String): Projection = Projection(List(Field(s)))
  def index(i: Int): Projection = Projection(List(Index(i)))

  def safeField(str: String): Option[Field] =
    if (str.contains(".") || str.contains("$")) None
    else Some(Field(str))

  def safeCartouches[A](inp: Map[CPathField, (CPathField, A)]): Option[Map[Field, (Field, A)]] =
    inp.toList.foldLeftM(Map.empty[Field, (Field, A)]){ (acc, x) =>
      x match {
        case (alias, (focus, lst)) => for {
          newAlias <- safeField(alias.name)
          newFocus <- safeField(focus.name)
        } yield acc.updated(newAlias, (newFocus, lst))
      }
    }

  def fromCPath(cpath: CPath): Option[Projection] = {
    val steps = cpath.nodes.foldMapM {
      case CPathField(str) => safeField(str).map(List(_))
      case CPathIndex(ix) => Some(List(Index(ix)))
      case _ => None
    }
    steps.map(Projection(_))
  }

  trait Grouped extends Product with Serializable

  object Grouped {
    final case class IndexGroup(i: Int) extends Grouped
    final case class FieldGroup(s: List[String]) extends Grouped

    def apply(p: Projection): List[Grouped] = {
      val accum = p.steps.foldLeft((List[String](), List[Grouped]())) { (acc, step) =>
        acc match {
          case (fldAccum, acc) => step match {
            case Field(s) => (s :: fldAccum, acc)
            case Index(i) => (List(), IndexGroup(i) :: FieldGroup(fldAccum.reverse) :: acc)
          }
        }
      }
      val reversed = accum._1 match {
        case List() => accum._2.reverse
        case x => (FieldGroup(x.reverse) :: accum._2).reverse
      }
      reversed.reverse
    }
  }

  trait Optics[A, O] {
    val proj: Prism[O, Const[Projection, A]]

    val _projection =
      Iso[Const[Projection, A], Projection](_.getConst)(Const(_))
    val projection =
      proj composePrism _projection.asPrism

    val _steps =
      Iso[Projection, List[Step]](_.steps)(Projection(_))

    val steps =
      projection composeIso _steps

    val key: Prism[O, String] =
      steps composePrism {
        Prism.partial[List[Step], String] {
          case Field(s) :: List() => s
        } { s => List(Field(s)) }
      }
  }

  implicit val renderTreeProjection: RenderTree[Projection] = RenderTree.make { x =>
    NonTerminal(List("Projection"), None, x.steps map (_.render))
  }
}
