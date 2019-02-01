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
import cats.instances.option._

import quasar.ParseInstruction

import org.bson.BsonValue

import quasar.physical.mongo.{Aggregator, Version, MongoProjection, MongoExpression}

import shims._

object Ids {
  def apply(
      uniqueKey: String,
      version: Version,
      processed: List[ParseInstruction])
      : Option[(List[Aggregator], BsonValue => BsonValue)] = {
    if (!processed.find(x => (ParseInstruction.Ids: ParseInstruction) === x).isEmpty) None
    else {
      val mapper = (x: BsonValue) => x.asDocument().get(uniqueKey)
      val aggregator: Option[Aggregator] =
        if (version > Version(3, 4, 0))
          Some(Aggregator.replaceRootWithList(
            uniqueKey,
            MongoExpression.MongoArray(List(MongoProjection.Id.toVar, MongoProjection.Root.toVar))))
        else if (version > Version(3, 2, 0))
          Some(Aggregator.project(MongoExpression.MongoObject(Map(
            uniqueKey -> MongoExpression.MongoArray(List(MongoProjection.Id.toVar, MongoProjection.Root.toVar))
          ))))
        else
          None
      aggregator map (x => (List(x), mapper))
    }
  }
}
