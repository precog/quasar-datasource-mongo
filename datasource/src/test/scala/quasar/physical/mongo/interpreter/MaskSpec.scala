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

import matryoshka.data.Fix

import org.specs2.mutable.Specification

import quasar.api.table.ColumnType
import quasar.common.CPath
import quasar.physical.mongo.expression.{O => _, Expr => _, _}

import Compiler._

class MaskSpec extends Specification with quasar.TreeMatchers {
  "examples" >> {
    def evalMask(masks: Map[CPath, Set[ColumnType]]): Option[List[Pipe]] =  Mask("root", masks)

    val O = Optics.coreOpT[Fix, CoreOp]
    type Expr = Fix[CoreOp]

    val rootKey = O.string("$root")

    val undefined = O.string("$root_non_existent_field")

    def typeEq(prj: Expr, s: String): Expr =
      O.$eq(List(O.$type(prj), O.string(s)))

    def isObjectFilter(x: Expr, y: Expr): Expr =
      O.$cond(typeEq(x, "object"), y, undefined)

    def pipeEqual(a: Option[List[Pipe]], b: Expr) = {
      a must beLike {
        case Some(x :: Pipeline.NotNull("root") :: List()) =>
          toCoreOp(eraseCustomPipeline(x)) must beTree(b)
      }
    }

    def wrapWithProject(e: Expr) = O.obj(Map("$project" -> e))
    def wrapWithMatch(e: Expr) = O.obj(Map("$match" -> e))

    "drop everything when empty" >> {
      evalMask(Map.empty) must beLike {
        case Some(List(x)) =>
          val expected = wrapWithMatch(O.obj(Map("root_non_existent_field" -> O.bool(false))))
          toCoreOp(eraseCustomPipeline(x)) must beTree(expected)
      }
    }

    "numbers and booleans types at identity" >> {
      val actual = evalMask(Map(CPath.Identity -> Set(ColumnType.Number, ColumnType.Boolean)))

      val expected = wrapWithProject(O.obj(Map("root" -> O.$cond(
        O.$or(List(
            typeEq(rootKey, "double"),
            typeEq(rootKey, "long"),
            typeEq(rootKey, "int"),
            typeEq(rootKey, "decimal"),
            typeEq(rootKey, "bool"))),
          rootKey,
          undefined))))
      pipeEqual(actual, expected)
    }

    "objects at identity" >> {
      val actual = evalMask(Map(CPath.Identity -> Set(ColumnType.Object)))
      val expected = wrapWithProject(O.obj(Map("root" -> O.$cond(typeEq(rootKey, "object"), rootKey, undefined))))
      pipeEqual(actual, expected)
    }

    "mask at path" >> {
      val actual = evalMask(Map(CPath.parse(".a.b") -> Set(ColumnType.String)))

      def abFilter(x: Expr): Expr =
        O.$cond(typeEq(O.string("$root.a.b"), "string"), x, undefined)

      val expected =
        wrapWithProject(O.obj(Map(
          "root" -> abFilter(
            isObjectFilter(rootKey, O.obj(Map("a" ->
              abFilter(isObjectFilter(O.string("$root.a"), O.obj(Map("b" ->
                abFilter(O.string("$root.a.b")))))))))))))
      pipeEqual(actual, expected)
    }

    "composition" >> {
      val actual = evalMask(Map(
        CPath.parse(".a.c") -> Set(ColumnType.Boolean),
        CPath.parse(".a") -> Set(ColumnType.Array)))

      def acFilter(x: Expr) =
        O.$cond(typeEq(O.string("$root.a.c"), "bool"), x, undefined)

      def bothFilters(x: Expr) =
        O.$cond(O.$or(List(typeEq(O.string("$root.a"), "array"), typeEq(O.string("$root.a.c"), "bool"))), x, undefined)

      val expected = wrapWithProject(O.obj(Map(
        "root" -> bothFilters(
          isObjectFilter(rootKey, O.obj(Map("a" ->
            bothFilters(
              O.$cond(
                typeEq(O.string("$root.a"), "object"),
                O.obj(Map("c" -> acFilter(O.string("$root.a.c")))),
                O.string("$root.a"))))))))))

      pipeEqual(actual, expected)
    }

    "erasing objects check, array check and preserving arrays" >> {
      val actual = evalMask(Map(
        CPath.parse(".a") -> Set(ColumnType.Object),
        CPath.parse(".a.b") -> Set(ColumnType.Boolean),
        CPath.parse(".c") -> Set(ColumnType.Array),
        CPath.parse(".c[1]") -> Set(ColumnType.Number, ColumnType.String),
        CPath.parse(".d[1]") -> Set(ColumnType.Boolean)))

      def indexed(e: Expr, ix: Int): Expr =
        O.$let(Map("level1" -> e), O.$arrayElemAt(O.string("$$level1"), ix))

      val expected = wrapWithProject(O.obj(Map(
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
                  undefined),
              "d" ->
                O.$cond(
                  typeEq(indexed(O.string("$root.d"), 1), "bool"),
                  O.$cond(
                    typeEq(O.string("$root.d"), "array"),
                    O.array(List(
                      O.$cond(
                        typeEq(indexed(O.string("$root.d"), 1), "bool"),
                        indexed(O.string("$root.d"), 1),
                        undefined))),
                    undefined),
                  undefined),
              "a" ->
                O.$cond(
                  typeEq(O.string("$root.a"), "object"),
                  O.string("$root.a"),
                  undefined))),
            undefined),
          undefined))))

      pipeEqual(actual, expected)
    }
  }


}
