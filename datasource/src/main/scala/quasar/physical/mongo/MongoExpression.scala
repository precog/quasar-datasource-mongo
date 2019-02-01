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

import org.bson._

import scala.collection.JavaConverters._

trait MongoExpression {
  def toBsonValue: BsonValue
}

trait MongoProjection extends MongoExpression {
  def toVar: MongoProjection.Field
}

trait MongoConstruct extends MongoExpression

object MongoProjection {
  val Id: MongoProjection = Field("_id")
  val Root: MongoProjection = Field("$ROOT")

  final case class Field(field: String) extends MongoProjection {
    def toBsonValue: BsonValue = new BsonString(field)
    def toVar: Field = Field("$" ++ field)
  }
  final case class Index(index: Int) extends MongoProjection {
    def toBsonValue: BsonValue = new BsonInt32(index)
    def toVar: Field = Field("$" ++ index.toString)
  }
}

object MongoExpression {
  final case class MongoArray(projections: List[MongoExpression]) extends MongoConstruct {
    def toBsonValue: BsonValue = new BsonArray((projections map (_.toBsonValue)).toSeq.asJava)
  }
  final case class MongoObject(projectionMap: Map[String, MongoExpression]) extends MongoConstruct {
    def toBsonValue: BsonValue = {
      val elements = projectionMap map {
        case (key, value) => new BsonElement(key, value.toBsonValue)
      }
      new BsonDocument(elements.toSeq.asJava)
    }
  }
}
