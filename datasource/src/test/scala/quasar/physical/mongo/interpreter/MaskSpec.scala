/*
 * Copyright 2020 Precog Data
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

import org.specs2.mutable.Specification

import quasar.api.ColumnType
import quasar.RenderTree._
import quasar.common.CPath
import quasar.contrib.iotac._
import quasar.ScalarStage
import quasar.physical.mongo.expression._

import cats.implicits._
import cats.mtl.implicits._
import higherkindness.droste.data.Fix

import Compiler._

class MaskSpec extends Specification with quasar.TreeMatchers {
  val o = Optics.coreOp(Optics.basisPrism[CoreOp, Fix[CoreOp]])

  def typeEq(prj: Fix[CoreOp], s: String): Fix[CoreOp] =
    o.eqx(List(o.typ(prj), o.str(s)))

  def pipeEqualWithKey(key: String, a: Option[List[Pipeline[Fix[Expr]]]], b: Fix[CoreOp]) = {
    a must beLike {
      case Some(x :: Pipeline.Presented :: List()) =>
        eraseCustomPipeline[Expr, Fix[Expr]](key, x) must beLike {
          case List(pipe) =>
            val pipeObjs = pipelineObjects[Expr, Fix[Expr]](_)
            val projections = compileProjections[Fix[Expr], Fix[CoreOp]](key)
            (pipeObjs andThen projections andThen letOpt andThen  orOpt)(pipe) must beTree(b)
        }
    }
  }

  def wrapWithProject(e: Fix[CoreOp]) = o.obj(Map("$project" -> e))
  def wrapWithMatch(e: Fix[CoreOp]) = o.obj(Map("$match" -> e))

  def eval(state: InterpretationState, masks: Map[CPath, Set[ColumnType]]): Option[(Mapper, List[Pipeline[Fix[Expr]]])] =
    Mask[InState, Fix[Expr]].apply(ScalarStage.Mask(masks)) run state map (_ leftMap (_.mapper))

  "state modifications" >> {
    "unfocused non root" >> {
      val state = InterpretationState("unique", Mapper.Unfocus)
      val result = eval(state, Map(CPath.parse("foo") -> Set(ColumnType.String)))
      val expected = wrapWithProject(o.obj(Map(
        "_id" -> o.int(0),
        "unique" -> o.obj(Map(
          "foo" -> o.cond(
            o.or(List(
              o.eqx(List(o.typ(o.str("$foo")), o.str("string"))),
              o.eqx(List(o.typ(o.str("$foo")), o.str("objectId"))),
              o.eqx(List(o.typ(o.str("$foo")), o.str("binData"))),
              o.eqx(List(o.typ(o.str("$foo")), o.str("dbPointer"))),
              o.eqx(List(o.typ(o.str("$foo")), o.str("symbol"))),
              o.eqx(List(o.typ(o.str("$foo")), o.str("javascript"))),
              o.eqx(List(o.typ(o.str("$foo")), o.str("javascriptWithScope"))),
              o.eqx(List(o.typ(o.str("$foo")), o.str("regex"))))),
            o.str("$foo"),
            o.str("$unique_missing")))))))
      pipeEqualWithKey("unique", result map (_._2), expected) and (
        (result map (_._1)) === Some(Mapper.Focus("unique")))
    }

    "unfocused root" >> {
      val state = InterpretationState("unique", Mapper.Unfocus)
      val result = eval(state, Map(CPath.Identity -> Set(ColumnType.Object)))
      val expected = wrapWithProject(o.obj(Map("_id" -> o.int(0), "unique" -> o.str("$$ROOT"))))
      pipeEqualWithKey("unique", result map (_._2), expected) and (
        (result map (_._1)) === Some(Mapper.Focus("unique")))
    }

    "focused" >> {
      val state = InterpretationState("other", Mapper.Focus("other"))
      val result = eval(state, Map(CPath.Identity -> Set(ColumnType.Object)))
      val expected = wrapWithProject(o.obj(Map(
        "_id" -> o.int(0),
        "other" -> o.cond(
          typeEq(o.str("$other"), "object"),
          o.str("$other"),
          o.str("other_missing")))))
      pipeEqualWithKey("other", result map (_._2), expected) and (
        (result map (_._1)) === Some(Mapper.Focus("other")))
    }
  }

  "examples" >> {
    val rootKey = o.str("$root")
    val missingKey = o.str("$root_missing")
    val missing = o.str("root_missing")

    def pipeEqual(a: Option[List[Pipeline[Fix[Expr]]]], b: Fix[CoreOp]) = pipeEqualWithKey("root", a, b)

    def evalMask(masks: Map[CPath, Set[ColumnType]]): Option[List[Pipeline[Fix[Expr]]]] =
      eval(InterpretationState("root", Mapper.Focus("root")), masks) map (_._2)

    "drop everything when empty" >> {
      evalMask(Map.empty) must beLike {
        case Some(List(x)) =>
          val expected = wrapWithMatch(o.obj(Map("root_erase" -> o.bool(false))))
          eraseCustomPipeline[Expr, Fix[Expr]]("root", x) must beLike {
            case List(pipe) =>
              val pipeObjs = pipelineObjects[Expr, Fix[Expr]](_)
              val projections = compileProjections[Fix[Expr], Fix[CoreOp]]("root")
              (pipeObjs andThen projections andThen letOpt andThen orOpt)(pipe) must beTree(expected)
          }
      }
    }

    "numbers and booleans types at identity" >> {
      val actual = evalMask(Map(CPath.Identity -> Set(ColumnType.Number, ColumnType.Boolean)))

      val expected = wrapWithProject(o.obj(Map(
        "_id" -> o.int(0),
        "root" -> o.cond(
          o.or(List(
            typeEq(rootKey, "double"),
            typeEq(rootKey, "long"),
            typeEq(rootKey, "int"),
            typeEq(rootKey, "decimal"),
            typeEq(rootKey, "bool"))),
          rootKey,
          missing))))
      pipeEqual(actual, expected)
    }

    "objects at identity" >> {
      val actual = evalMask(Map(CPath.Identity -> Set(ColumnType.Object)))
      val expected = wrapWithProject(o.obj(Map(
        "_id" -> o.int(0),
        "root" -> o.cond(typeEq(rootKey, "object"), rootKey, missing))))
      pipeEqual(actual, expected)
    }

    "mask at path" >> {
      val actual = evalMask(Map(CPath.parse(".a.b") -> Set(ColumnType.String)))

      val abCheck = o.or(List(
        typeEq(o.str("$root.a.b"), "string"),
        typeEq(o.str("$root.a.b"), "objectId"),
        typeEq(o.str("$root.a.b"), "binData"),
        typeEq(o.str("$root.a.b"), "dbPointer"),
        typeEq(o.str("$root.a.b"), "symbol"),
        typeEq(o.str("$root.a.b"), "javascript"),
        typeEq(o.str("$root.a.b"), "javascriptWithScope"),
        typeEq(o.str("$root.a.b"), "regex")))

      val expected =
        wrapWithProject(o.obj(Map(
          "_id" -> o.int(0),
          "root" ->
            o.cond(
              abCheck,
              o.cond(
                typeEq(o.str("$root"), "object"),
                o.obj(Map("a" ->
                  o.cond(
                    abCheck,
                    o.cond(
                      typeEq(o.str("$root.a"), "object"),
                      o.obj(Map("b" ->
                        o.cond(
                          abCheck,
                          o.str("$root.a.b"),
                          missingKey))),
                      missingKey),
                    missingKey))),
                missing),
              missing))))
      pipeEqual(actual, expected)
    }

    "composition" >> {
      val actual = evalMask(Map(
        CPath.parse(".a.c") -> Set(ColumnType.Boolean),
        CPath.parse(".a") -> Set(ColumnType.Array)))

      val acCheck =
        typeEq(o.str("$root.a.c"), "bool")

      val bothCheck = o.or(List(
        typeEq(o.str("$root.a"), "array"),
        acCheck))

      val expected = wrapWithProject(o.obj(Map(
        "_id" -> o.int(0),
        "root" ->
          o.cond(
            bothCheck,
            o.cond(
              typeEq(o.str("$root"), "object"),
              o.obj(Map("a" ->
                o.cond(
                  bothCheck,
                  o.cond(
                    typeEq(o.str("$root.a"), "object"),
                    o.obj(Map("c" -> o.cond(acCheck, o.str("$root.a.c"), missingKey))),
                    o.str("$root.a")),
                  missingKey))),
              missing),
            missing))))

      pipeEqual(actual, expected)
    }
    "erasing objects check, array check and preserving arrays" >> {
      val actual = evalMask(Map(
        CPath.parse(".a") -> Set(ColumnType.Object),
        CPath.parse(".a.b") -> Set(ColumnType.Boolean),
        CPath.parse(".c") -> Set(ColumnType.Array),
        CPath.parse(".c[1]") -> Set(ColumnType.Number, ColumnType.String),
        CPath.parse(".d[1]") -> Set(ColumnType.Boolean)))

      def indexed(e: Fix[CoreOp], ix: Int): Fix[CoreOp] = o.let(
        Map("level0" -> e),
        o.cond(
          o.eqx(List(o.typ(e), o.str("array"))),
          o.arrayElemAt(o.str("$$level0"), ix),
          missing))

      val expected = wrapWithProject(o.obj(Map(
        "_id" -> o.int(0),
        "root" -> o.cond(
          o.or(List(
            typeEq(o.str("$root.c"), "array"),
            typeEq(indexed(o.str("$root.d"), 1), "bool"),
            typeEq(o.str("$root.a"), "object"))),
          o.cond(
            typeEq(rootKey, "object"),
            o.obj(Map(
              "c" ->
                o.cond(
                  typeEq(o.str("$root.c"), "array"),
                  o.str("$root.c"),
                  missingKey),
              "d" ->
                o.cond(
                  typeEq(indexed(o.str("$root.d"), 1), "bool"),
                  o.cond(
                    typeEq(o.str("$root.d"), "array"),
                    o.reduce(
                      o.array(List(
                        o.cond(
                          typeEq(indexed(o.str("$root.d"), 1), "bool"),
                          indexed(o.str("$root.d"), 1),
                          missing))),
                      o.array(List()),
                      o.cond(
                        o.eqx(List(o.str("$$this"), o.str("root_missing"))),
                        o.str("$$value"),
                        o.concatArrays(List(o.str("$$value"), o.array(List(o.str("$$this"))))))),
                    missingKey),
                  missingKey),
              "a" ->
                o.cond(
                  typeEq(o.str("$root.a"), "object"),
                  o.str("$root.a"),
                  missingKey))),
            missing),
          missing))))

      pipeEqual(actual, expected)
    }
  }
}
