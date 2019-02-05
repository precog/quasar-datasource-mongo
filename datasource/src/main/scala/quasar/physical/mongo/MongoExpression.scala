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
import quasar.common.{CPath, CPathNode, CPathField, CPathMeta, CPathIndex, CPathArray}

import monocle.{Iso, PTraversal, Prism}


trait MongoExpression {
  def toBsonValue: BsonValue
}

trait MongoProjection extends MongoExpression {
  def toString: String
  def toVar: MongoProjection.Field
  def +/(prj: MongoProjection): MongoProjection
}

trait MongoConstruct extends MongoExpression

// TODO: nice projection syntax
object MongoProjection {
  val Id: MongoProjection = Field("_id")
  val Root: MongoProjection = Field("$ROOT")

  final case class Field(field: String) extends MongoProjection {
    override def toString = field
    def toBsonValue: BsonValue = new BsonString(field)
    def toVar: Field = Field("$" ++ field)
    def +/(prj: MongoProjection) = this
  }

  def field: Prism[MongoProjection, String] =
    Prism.partial[MongoProjection, String] {
      case Field(str) => str
    } ( x => Field(x) )


  final case class Index(index: Int) extends MongoProjection {
    override def toString = index.toString
    def toBsonValue: BsonValue = new BsonInt32(index)
    def toVar: Field = Field("$" ++ index.toString)
    def +/(prj: MongoProjection) = this
  }

  def index: Prism[MongoProjection, Int] =
    Prism.partial[MongoProjection, Int] {
      case Index(i) => i
    } ( i => Index(i) )
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

  final case object MongoNull extends MongoExpression {
    def toBsonValue: BsonValue = new BsonNull
  }

  final case class MongoBoolean(b: Boolean) extends MongoExpression {
    def toBsonValue: BsonValue = new BsonBoolean(b)
  }

  final case class MongoInt(i: Int) extends MongoExpression {
    def toBsonValue: BsonValue = new BsonInt32(i)
  }

  final case class MongoString(str: String) extends MongoExpression {
    def toBsonValue: BsonValue = new BsonString(str)
  }
}
