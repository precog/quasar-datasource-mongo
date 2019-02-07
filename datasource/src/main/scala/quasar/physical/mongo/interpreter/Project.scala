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
import cats.syntax.foldable._
import cats.instances.list._
import cats.instances.option._

import quasar.common.{CPath, CPathField, CPathIndex, CPathNode}

import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}

import shims._

object Project {
  def apply(
      uniqueKey: String,
      version: Version,
      path: CPath)
      : Option[List[Aggregator]] = {

    def filterGuard(prj: E.Projection): Boolean = {
      val hasNotIx: Boolean = prj.steps.toList.forall {
        case E.Index(_) => true
        case E.Field(_) => false
      }
      version > (if (hasNotIx) Version(0, 0, 0) else Version(3, 2, 0)) // Not sure if $let works for < 2.0
    }

    E.cpathToProjection(path) flatMap { (fld: E.Projection) =>
      if (!filterGuard(fld)) None else Some {
        val projection = E.key(uniqueKey) +/ fld
        val match_ =
          Aggregator.filter(E.Object(projection.toKey -> E.Object("$exists" -> E.Bool(true))))
        val project =
          Aggregator.project(E.Object(uniqueKey -> projection))
        List(match_, project)
      }
    }

  }
}
