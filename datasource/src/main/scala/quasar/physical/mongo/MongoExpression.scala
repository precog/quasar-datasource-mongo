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

import cats.syntax.foldable._
import cats.instances.option._
import cats.instances.list._

import org.bson._

import scala.collection.JavaConverters._

import quasar.common.{CPath, CPathField, CPathIndex}

trait MongoExpression {
  def toBsonValue: BsonValue
}

object MongoExpression {
  import slamdata.Predef.{String => SString, Int => SInt}

  trait ProjectionStep
  final case class Field(str: SString) extends ProjectionStep
  final case class Index(int: SInt) extends ProjectionStep

  def keyPart(projectionStep: ProjectionStep): SString = projectionStep match {
    case Field(str) => str
    case Index(int) => int.toString
  }

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

    def depth: SInt = steps.length
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

  def cpathToProjection(cpath: CPath): Option[Projection] = {
    cpath.nodes.foldM(Projection()) { (acc, node) => node match {
      case CPathField(str) => Some(acc +/ key(str))
      case CPathIndex(ix) => Some(acc +/ index(ix))
      case _ => None
    }}
  }

  object helpers {
    def cond(test: MongoExpression, ok: MongoExpression, fail: MongoExpression): Object =
      Object("$$cond" -> Array(test, ok, fail))

    def or(lst: List[MongoExpression]): Object =
      Object("$$or" -> Array(lst:_*))

    def equal(a: MongoExpression, b: MongoExpression): MongoExpression =
      Object("$$eq" -> Array(a, b))

    def isObject(proj: Projection): MongoExpression =
      equal(typeExpr(proj), String("object"))

    def isArray(proj: Projection): MongoExpression =
      equal(typeExpr(proj), String("array"))

    def typeExpr(proj: Projection): MongoExpression =
      Object("$$type" -> proj)

    def onIndex(proj: Projection, ix: SInt, fn: (MongoExpression => MongoExpression)): MongoExpression = {
      val indexProj = key("$$value") +/ key("index")
      val resultProj = key("$$value") +/ key("result")
      val thisProj = key("$$this")
      val applied = cond(equal(indexProj, Int(ix)), fn(thisProj), thisProj)
      val in = Object(
        "index" -> Object("$sum" -> Array(indexProj, Int(1))),
        "result" -> Object("arrayConcat" -> Array(resultProj, Array(Array(applied))))
      )
      val reduce = Object("$$reduce" -> Object(
        "input" -> proj,
        "initialValue" -> Object("result" -> Array(), "index" -> Int(0)),
        "in" -> in
      ))
      val result = Let(Object("reduce" -> reduce), key("$$reduce") +/ key("result"))
      cond(isArray(proj), result, proj)
    }
  }
}
