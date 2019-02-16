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

import quasar.common.CPath

import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}

import shims._

object Project {
  def apply(
      uniqueKey: String,
      version: Version,
      path: CPath)
      : Option[List[Aggregator]] = {

    E.cpathToProjection(path) flatMap { (fld: E.Projection) =>
      if (fld.minVersion > version) None else Some {
        val tmpKey =
          uniqueKey.concat("_project")
        val projection =
          E.key(uniqueKey) +/ fld
        val match_ =
          Aggregator.filter(E.Object(projection.toKey -> E.Object("$exists" -> E.Bool(true))))
        val move =
          Aggregator.project(E.Object(tmpKey -> projection))
        val project =
          Aggregator.project(E.Object(uniqueKey -> E.key(tmpKey)))
        List(match_, move, project)
      }
    }

  }
}
