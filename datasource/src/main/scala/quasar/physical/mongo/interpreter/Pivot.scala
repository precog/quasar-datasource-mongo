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

package quasar.physical.mongo.interpreter

import slamdata.Predef._

import cats.syntax.order._

import quasar.{CompositeParseType, IdStatus, ParseType}

import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}

import shims._

object Pivot {
  trait Pivotable
  final case object AsObject extends Pivotable
  final case object AsArray extends Pivotable

  def pivotable(structure: CompositeParseType): Option[Pivotable] = structure match {
    case ParseType.Array => Some(AsArray)
    case ParseType.Object => Some(AsObject)
    case _ => None
  }

  def pivotableTypeString(p: Pivotable): String = p match {
    case AsObject => "object"
    case AsArray => "array"
  }

  val IndexKey: String = "index"

  def matchStructure(path: E.Projection, p: Pivotable): Aggregator =
    Aggregator.filter(E.Object(path.toKey -> E.Object("$$type" -> E.String(pivotableTypeString(p)))))

  def toArray(projection: E.Projection, p: Pivotable): List[Aggregator] = p match {
    case AsArray => List()
    case AsObject => List(Aggregator.project(E.Object(projection.toKey -> E.Object("$$objectToArray" -> projection))))
  }

  def mkValue(status: IdStatus, p: Pivotable, unwindedKey: E.Projection): MongoExpression = p match {
    case AsArray => status match {
      case IdStatus.IdOnly => E.key(IndexKey)
      case IdStatus.ExcludeId => unwindedKey
      case IdStatus.IncludeId => E.Array(E.key(IndexKey), unwindedKey)
    }
    case AsObject => status match {
      case IdStatus.IdOnly => unwindedKey +/ E.key("k")
      case IdStatus.ExcludeId => unwindedKey +/ E.key("v")
      case IdStatus.IncludeId => E.Array(unwindedKey +/ E.key("k"), unwindedKey +/ E.key("v"))
    }
  }

  def apply(
      uniqueKey: String,
      version: Version,
      status: IdStatus,
      structure: CompositeParseType)
      : Option[List[Aggregator]] = {

    if (version < Version(3, 4, 0)) None
    else pivotable(structure) map { p =>
      val projection = E.key(uniqueKey)
      val filter = matchStructure(projection, p)
      val mkArray = toArray(projection, p)
      val unwind = Aggregator.unwind(projection, IndexKey)
      val toSet = mkValue(status, p, projection)
      val setProjection = Aggregator.project(E.Object(projection.toKey -> toSet))
      List(filter) ++ mkArray ++ List(unwind, setProjection)
    }
  }
}
