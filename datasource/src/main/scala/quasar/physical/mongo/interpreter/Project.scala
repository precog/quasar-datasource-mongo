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

import cats.syntax.foldable._
import cats.instances.list._
import cats.instances.option._

import quasar.ParseInstruction
import quasar.common.{CPath, CPathField, CPathIndex, CPathNode}

import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}

import shims._

object Project {
  def apply(
      uniqueKey: String,
      version: Version,
      processed: List[ParseInstruction],
      path: CPath)
      : Option[List[Aggregator]] = {

    val projectField: Option[E.Projection] =
      path.nodes.foldM(E.key(uniqueKey)) { (pt: E.Projection, node: CPathNode) => node match {
        case CPathField(str) => Some(pt +/ E.key(str))
        case CPathIndex(ix) => Some(pt +/ E.index(ix))
        case _ => None
      }}

    projectField map { (fld: E.Projection) =>
      val projection =
        E.key(uniqueKey) +/ fld
      val match_ =
        Aggregator.filter(E.Object(projection.toKey -> E.Object("$$exists" -> E.Bool(true))))
      val project =
        Aggregator.project(E.Object(uniqueKey -> projection))

      List(match_, project)
    }

  }
}
