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

import org.bson._

import matryoshka.birecursiveIso
import matryoshka.data.Fix

import quasar.physical.mongo.{Aggregator, Version, MongoExpression => E}

object Wrap {
  def apply(
      uniqueKey: String,
      version: Version,
      name: String)
      : Option[List[Aggregator]] =
    Some {
      val tmpKey = uniqueKey.concat("_wrap")
      List(
        Aggregator.project(E.Object(tmpKey -> E.Object(name -> E.key(uniqueKey)))),
        Aggregator.project(E.Object(uniqueKey -> E.key(tmpKey))))
    }

  import quasar.physical.mongo.{Expression, Optics, CustomPipeline, MongoPipeline, Pipeline}, Expression._
  def apply0(uniqueKey: String, name: String): Option[List[Pipeline[Fix[Projected]]]] = {
    val O = Optics.full(birecursiveIso[Fix[Projected], Projected].reverse.asPrism)
    val tmpKey = uniqueKey.concat("_wrap")
    Some(List(
      Pipeline.$project(Map(
        tmpKey -> O.obj(Map(name -> O.key(uniqueKey))))),
      Pipeline.$project(Map(
        uniqueKey -> O.key(tmpKey)))
    ))
  }

}
