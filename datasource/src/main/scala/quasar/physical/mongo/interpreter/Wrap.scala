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

import quasar.ParseInstruction
import quasar.common.CPath

import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}

import shims._

object Wrap {
  import Focus._

  def apply(
      uniqueKey: String,
      version: Version,
      processed: List[ParseInstruction],
      path: CPath,
      name: String)
      : Option[List[Aggregator]] = {

    if (version < Version(3, 4, 0)) None
    else E.cpathToProjection(path) map { prj =>
      val fs = focuses(E.key(uniqueKey) +/ prj)
      val wrapped = E.Object(name -> prj)
      setByFocuses(wrapped, fs._1, fs._2)
    }
  }

}
