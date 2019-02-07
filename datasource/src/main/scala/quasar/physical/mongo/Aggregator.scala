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

import slamdata.Predef._

import monocle.Prism

import org.mongodb.scala._

trait Aggregator {
  def toDocument: Document
}

object Aggregator {
  val E = MongoExpression

  final case class Project(obj: E.Object) extends Aggregator {
    def toDocument: Document = Document(s"$$project" -> obj.toBsonValue)
  }

  final case class Unwind(path: E.Projection, includeArrayIndex: String) extends Aggregator {
    def toDocument: Document =
      Document(s"$$unwind" -> Document(
        "path" -> E.String("$" ++ path.toKey).toBsonValue,
        "includeArrayIndex" -> includeArrayIndex,
        "preserveNullAndEmptyArrays" -> true))
  }

  final case class Filter(obj: E.Object) extends Aggregator {
    def toDocument: Document =
      Document("$match" -> obj.toBsonValue)
  }

  def filter: Prism[Aggregator, E.Object] =
    Prism.partial[Aggregator, E.Object] {
      case Filter(obj) => obj
    } ( x => Filter(x) )

  def unwind: Prism[Aggregator, (E.Projection, String)] =
    Prism.partial[Aggregator, (E.Projection, String)] {
      case Unwind(p, i) => (p, i)
    } { case (p, i) => Unwind(p, i) }

  def project: Prism[Aggregator, E.Object] =
    Prism.partial[Aggregator, E.Object] {
      case Project(obj) => obj
    } (x => Project(x))
}
