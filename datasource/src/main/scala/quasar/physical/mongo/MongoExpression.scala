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

trait ProjectionStep

object ProjectionStep {
  final case class Field(str: String) extends ProjectionStep
  final case class Index(int: Int) extends ProjectionStep

  def keyPart(projectionStep: ProjectionStep): String = projectionStep match {
    case Field(str) => str
    case Index(int) => int.toString
  }
}

object MongoExpression {
  import ProjectionStep._
  import slamdata.Predef.{String => SString, Int => SInt}

  final case class Projection(steps: ProjectionStep*) extends MongoExpression {
    def toKey: SString = steps.toList match {
      case List() => "$$ROOT"
      case hd :: tail => steps.foldLeft (keyPart(hd)) { (acc, x) => acc ++ "." ++ keyPart(x) }
    }

    private def levelString(ix: SInt): SString =
      s"level::${ix.toString}"

    @scala.annotation.tailrec
    private def toObj(accum: Let, level: SInt): Let = steps match {
      // all done
      case List() => accum
      case hd :: tail => hd match {
        // having field step
        case Field(str) => accum.in match {
          // we have something like {$let: {vars: ..., in: "foo.bar.baz"}}
          // the result is {$let: {vars: ..., in: "foo.bar.baz.newField"}}
          case String(fld) => accum.copy(in = String(fld ++ "." ++ str))
          // this case shouldn't happen but it's valid in case of let folding
          // input: {$let: {vars: ..., in: <expression>}}
          // output {$let: {vars: {"level::ix": {$let: {vars: ..., in: <expression>}}}, in: "$$level::ix.newField"}}
          case inLevel =>
            toObj(
              Let(
                Object(levelString(level) -> accum),
                String("$$$$" ++ levelString(level) ++ "." ++ str)),
              level + 1)
        }
        // input: {$let: {vars: ..., in: <expression>}}
        // output: {$let: {vars: {level::ix: {$arrayElemAt: [{$let: {vars: ..., in: <expression>}}, ix]}}, in: "$$level::ix"}}
        case Index(ix) =>
          toObj(
            Let(
              Object(levelString(level) -> Object("$$arrayElemAt" -> Array(accum, Int(ix)))),
              String("$$$$" ++ levelString(level))),
            level + 1)
      }
    }

    def toBsonValue: BsonValue = {
      // {$let: {vars: {level: "$$ROOT"}, in: "$$level}} is "$$ROOT"
      val initialLet = Let(Object("level" -> String("$$$$ROOT")), String("$$$$level"))
      toObj(initialLet, 0).toBsonValue
    }

    def +/(prj: Projection): Projection = Projection((steps ++ prj.steps):_*)
  }

  def key(s: SString): Projection = Projection(Field(s))

  def index(i: SInt): Projection = Projection(Index(i))

  final case class Let(vars: Object, in: MongoExpression) extends MongoExpression {
    def toObj: Object =
      Object("$$let" -> Object(
        "vars" -> vars,
        "in" -> in))
    def toBsonValue: BsonValue = this.toObj.toBsonValue
  }

  final case class Array(projections: MongoExpression*) extends MongoExpression {
    def toBsonValue: BsonValue = new BsonArray((projections map (_.toBsonValue)).asJava)
  }
  final case class Object(fields: (SString, MongoExpression)*) extends MongoExpression {
    def toBsonValue: BsonValue = {
      val elements = fields map {
        case (key, value) => new BsonElement(key, value.toBsonValue)
      }
      new BsonDocument(elements.asJava)
    }
  }

  final case class Bool(b: Boolean) extends MongoExpression {
    def toBsonValue: BsonValue = new BsonBoolean(b)
  }

  final case class Int(i: SInt) extends MongoExpression {
    def toBsonValue: BsonValue = new BsonInt32(i)
  }

  final case class String(str: SString) extends MongoExpression {
    def toBsonValue: BsonValue = new BsonString(str)
  }
}
