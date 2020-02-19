/*
 * Copyright 2014–2019 SlamData Inc.
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

import matryoshka.data.Fix

import org.specs2.mutable.Specification

import quasar.api.ColumnType
import quasar.common.CPath
import quasar.physical.mongo.expression.{O => _, Expr => _, _}

import scalaz.Scalaz._

import Compiler._

class MaskSpec extends Specification with quasar.TreeMatchers {
  val O = Optics.coreOpT[Fix, CoreOp]
  type Expr = Fix[CoreOp]

  def typeEq(prj: Expr, s: String): Expr =
    O.$eq(List(O.$type(prj), O.string(s)))

  def pipeEqualWithKey(key: String, a: Option[List[Pipe]], b: Expr) = {
    a must beLike {
      case Some(x :: Pipeline.Presented :: List()) =>
        eraseCustomPipeline(key, x) must beLike {
          case List(pipe) =>
            toCoreOp(key, pipe) must beTree(b)
        }
    }
  }

  def wrapWithProject(e: Expr) = O.obj(Map("$project" -> e))
  def wrapWithMatch(e: Expr) = O.obj(Map("$match" -> e))

  def eval(state: InterpretationState, masks: Map[CPath, Set[ColumnType]]): Option[(Mapper, List[Pipe])] =
    Mask[InState](masks) run state map (_ leftMap (_.mapper))

  "state modifications" >> {
    "unfocused non root" >> {
      val state = InterpretationState("unique", Mapper.Unfocus)
      val result = eval(state, Map(CPath.parse("foo") -> Set(ColumnType.String)))
      val expected = wrapWithProject(O.obj(Map(
        "_id" -> O.int(0),
        "unique" -> O.obj(Map(
          "foo" -> O.$cond(
            O.$or(List(
              O.$eq(List(O.$type(O.string("$foo")), O.string("string"))),
              O.$eq(List(O.$type(O.string("$foo")), O.string("objectId"))))),
            O.string("$foo"),
            O.string("$unique_missing")))))))
      pipeEqualWithKey("unique", result map (_._2), expected) and (
        (result map (_._1)) === Some(Mapper.Focus("unique")))
    }

    "unfocused root" >> {
      val state = InterpretationState("unique", Mapper.Unfocus)
      val result = eval(state, Map(CPath.Identity -> Set(ColumnType.Object)))
      val expected = wrapWithProject(O.obj(Map("_id" -> O.int(0), "unique" -> O.string("$$ROOT"))))
      pipeEqualWithKey("unique", result map (_._2), expected) and (
        (result map (_._1)) === Some(Mapper.Focus("unique")))
    }

    "focused" >> {
      val state = InterpretationState("other", Mapper.Focus("other"))
      val result = eval(state, Map(CPath.Identity -> Set(ColumnType.Object)))
      val expected = wrapWithProject(O.obj(Map(
        "_id" -> O.int(0),
        "other" -> O.$cond(
          typeEq(O.string("$other"), "object"),
          O.string("$other"),
          O.string("other_missing")))))
      pipeEqualWithKey("other", result map (_._2), expected) and (
        (result map (_._1)) === Some(Mapper.Focus("other")))
    }
  }

  "examples" >> {
    val rootKey = O.string("$root")
    val missingKey = O.string("$root_missing")
    val missing = O.string("root_missing")

    def pipeEqual(a: Option[List[Pipe]], b: Expr) = pipeEqualWithKey("root", a, b)

    def evalMask(masks: Map[CPath, Set[ColumnType]]): Option[List[Pipe]] =
      eval(InterpretationState("root", Mapper.Focus("root")), masks) map (_._2)

    "drop everything when empty" >> {
      evalMask(Map.empty) must beLike {
        case Some(List(x)) =>
          val expected = wrapWithMatch(O.obj(Map("root_erase" -> O.bool(false))))
          eraseCustomPipeline("root", x) must beLike {
            case List(pipe) => toCoreOp("root", pipe) must beTree(expected)
          }
      }
    }

    "numbers and booleans types at identity" >> {
      val actual = evalMask(Map(CPath.Identity -> Set(ColumnType.Number, ColumnType.Boolean)))

      val expected = wrapWithProject(O.obj(Map(
        "_id" -> O.int(0),
        "root" -> O.$cond(
          O.$or(List(
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
      val expected = wrapWithProject(O.obj(Map(
        "_id" -> O.int(0),
        "root" -> O.$cond(typeEq(rootKey, "object"), rootKey, missing))))
      pipeEqual(actual, expected)
    }

    "mask at path" >> {
      val actual = evalMask(Map(CPath.parse(".a.b") -> Set(ColumnType.String)))

      val abCheck = O.$or(List(
        typeEq(O.string("$root.a.b"), "string"),
        typeEq(O.string("$root.a.b"), "objectId")))

      val expected =
        wrapWithProject(O.obj(Map(
          "_id" -> O.int(0),
          "root" ->
            O.$cond(
              abCheck,
              O.$cond(
                typeEq(O.string("$root"), "object"),
                O.obj(Map("a" ->
                  O.$cond(
                    abCheck,
                    O.$cond(
                      typeEq(O.string("$root.a"), "object"),
                      O.obj(Map("b" ->
                        O.$cond(
                          abCheck,
                          O.string("$root.a.b"),
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
        typeEq(O.string("$root.a.c"), "bool")

      val bothCheck = O.$or(List(
        typeEq(O.string("$root.a"), "array"),
        acCheck))

      val expected = wrapWithProject(O.obj(Map(
        "_id" -> O.int(0),
        "root" ->
          O.$cond(
            bothCheck,
            O.$cond(
              typeEq(O.string("$root"), "object"),
              O.obj(Map("a" ->
                O.$cond(
                  bothCheck,
                  O.$cond(
                    typeEq(O.string("$root.a"), "object"),
                    O.obj(Map("c" -> O.$cond(acCheck, O.string("$root.a.c"), missingKey))),
                    O.string("$root.a")),
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

      def indexed(e: Expr, ix: Int): Expr = O.$let(
        Map("level1" -> e),
        O.$cond(
          O.$eq(List(O.$type(e), O.string("array"))),
          O.$arrayElemAt(O.string("$$level1"), ix),
          missing))

      val expected = wrapWithProject(O.obj(Map(
        "_id" -> O.int(0),
        "root" -> O.$cond(
          O.$or(List(
            typeEq(O.string("$root.c"), "array"),
            typeEq(indexed(O.string("$root.d"), 1), "bool"),
            typeEq(O.string("$root.a"), "object"))),
          O.$cond(
            typeEq(rootKey, "object"),
            O.obj(Map(
              "c" ->
                O.$cond(
                  typeEq(O.string("$root.c"), "array"),
                  O.string("$root.c"),
                  missingKey),
              "d" ->
                O.$cond(
                  typeEq(indexed(O.string("$root.d"), 1), "bool"),
                  O.$cond(
                    typeEq(O.string("$root.d"), "array"),
                    O.$reduce(
                      O.array(List(
                        O.$cond(
                          typeEq(indexed(O.string("$root.d"), 1), "bool"),
                          indexed(O.string("$root.d"), 1),
                          missing))),
                      O.array(List()),
                      O.$cond(
                        O.$eq(List(O.string("$$this"), O.string("root_missing"))),
                        O.string("$$value"),
                        O.$concatArrays(List(O.string("$$value"), O.array(List(O.string("$$this"))))))),
                    missingKey),
                  missingKey),
              "a" ->
                O.$cond(
                  typeEq(O.string("$root.a"), "object"),
                  O.string("$root.a"),
                  missingKey))),
            missing),
          missing))))

      pipeEqual(actual, expected)
    }
  }
}
