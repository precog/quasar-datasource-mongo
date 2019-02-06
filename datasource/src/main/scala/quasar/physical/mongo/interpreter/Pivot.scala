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
import quasar.common.CPath

import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}

import shims._

object Pivot {
  import Focus._

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

  val ToUnwind: String = "to_unwind"
  val IndexKey: String = "index"

  def matchStructure(path: E.Projection, p: Pivotable): Aggregator =
    Aggregator.filter(E.Object(path.toKey -> E.Object("$$type" -> E.String(pivotableTypeString(p)))))

  def moveStructure(projection: E.Projection, p: Pivotable): Aggregator = p match {
    case AsArray =>
      Aggregator.addFields(E.Object(ToUnwind -> projection))
    case AsObject =>
      Aggregator.addFields(E.Object(ToUnwind -> E.Object("$$objectToArray" -> projection)))
  }

  def mkValue(status: IdStatus, p: Pivotable): MongoExpression = p match {
    case AsArray => status match {
      case IdStatus.IdOnly => E.key(IndexKey)
      case IdStatus.ExcludeId => E.key(ToUnwind)
      case IdStatus.IncludeId => E.Array(E.key(IndexKey), E.key(ToUnwind))
    }
    case AsObject => status match {
      case IdStatus.IdOnly => E.key(ToUnwind) +/ E.key("k")
      case IdStatus.ExcludeId => E.key(ToUnwind) +/ E.key("v")
      case IdStatus.IncludeId => E.Array(E.key(ToUnwind) +/ E.key("k"), E.key(ToUnwind) +/ E.key("v"))
    }
  }

  def apply(
      uniqueKey: String,
      version: Version,
      path: CPath,
      status: IdStatus,
      structure: CompositeParseType)
      : Option[List[Aggregator]] = {

    if (version < Version(3, 4, 0)) None
    else for {
      fld <- E.cpathToProjection(path)
      p <- pivotable(structure)
    } yield {
      val projection = E.key(uniqueKey) +/ fld
      val filter = matchStructure(projection, p)
      val move = moveStructure(projection, p)
      val unwind = Aggregator.unwind(E.key(ToUnwind), IndexKey)
      val toSet = mkValue(status, p)
      val fs = focuses(projection)
      List(filter, move, unwind) ++ setByFocuses(toSet, fs._1, fs._2)
    }
  }
}
