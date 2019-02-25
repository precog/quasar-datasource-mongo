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
import org.specs2.mutable.Specification

import quasar.api.table.ColumnType
import quasar.common.CPath
import quasar.physical.mongo.Version
import quasar.physical.mongo.expression._

class MaskSpec extends Specification {
  "examples" >> {
    def evalMask(masks: Map[CPath, Set[ColumnType]]): Option[List[Pipe]] =  Mask.apply0("root", masks)

    val rootKey = O.key("root")

    val undefined = O.key("root_non_existent_field")

    def mkExpected(e: Map[String, Expr]) =
      Some(List(Pipeline.$project(e), Pipeline.NotNull("root")))

    def typeEq(prj: Expr, s: String): Expr =
      O.$eq(List(O.$type(prj), O.string(s)))

    def isObjectFilter(x: Expr, y: Expr): Expr =
      O.$cond(typeEq(x, "object"), y, undefined)

    def pipeEqual(a: Option[List[Pipe]], b: Option[List[Pipe]]) = {
      val compile: List[Pipe] => Option[List[BsonDocument]] = x => compilePipeline(Version.$objectToArray, x)
      val aExpr = a flatMap compile
      val bExpr = b flatMap compile
      aExpr === bExpr
    }

    "drop everything when empty" >> {
      val actual = evalMask(Map.empty)
      val expected = Some(List(Pipeline.$match(Map("root_non_existent_field" -> O.bool(true)))))
      pipeEqual(actual, expected)
    }

    "numbers and booleans types at identity" >> {
      val actual = evalMask(Map(CPath.Identity -> Set(ColumnType.Number, ColumnType.Boolean)))

      val expected = mkExpected(Map("root" -> O.$cond(
        O.$or(List(
            typeEq(rootKey, "double"),
            typeEq(rootKey, "long"),
            typeEq(rootKey, "int"),
            typeEq(rootKey, "decimal"),
            typeEq(rootKey, "bool"))),
          rootKey,
          undefined)))
      pipeEqual(actual, expected)
    }

    "objects at identity" >> {
      val actual = evalMask(Map(CPath.Identity -> Set(ColumnType.Object)))
      val expected = mkExpected(Map("root" -> O.$cond(typeEq(rootKey, "object"), rootKey, undefined)))
      pipeEqual(actual, expected)
    }

    "mask at path" >> {
      val actual = evalMask(Map(CPath.parse(".a.b") -> Set(ColumnType.String)))
      val aKey = Projection.key("root") + Projection.key("a")
      val abKey = aKey + Projection.key("b")

      def abFilter(x: Expr): Expr =
        O.$cond(typeEq(O.projection(abKey), "string"), x, undefined)

      val expected =
        mkExpected(Map(
          "root" -> abFilter(
            isObjectFilter(rootKey, O.obj(Map("a" ->
              abFilter(isObjectFilter(O.projection(aKey), O.obj(Map("b" ->
                abFilter(O.projection(abKey))))))))))))
      pipeEqual(actual, expected)
    }

    "composition" >> {
      val actual = evalMask(Map(
        CPath.parse(".a.c") -> Set(ColumnType.Boolean),
        CPath.parse(".a") -> Set(ColumnType.Array)))

      // To me having the keys duplicated here is just coincidence, since they're defined for this very test
      val aKey = Projection.key("root") + Projection.key("a")
      val acKey = aKey + Projection.key("c")

      def acFilter(x: Expr) =
        O.$cond(typeEq(O.projection(acKey), "bool"), x, undefined)

      def bothFilters(x: Expr) =
        O.$cond(O.$or(List(typeEq(O.projection(aKey), "array"), typeEq(O.projection(acKey), "bool"))), x, undefined)

      val expected = mkExpected(Map(
        "root" -> bothFilters(
          isObjectFilter(rootKey, O.obj(Map("a" ->
            bothFilters(
              O.$cond(
                typeEq(O.projection(aKey), "object"),
                O.obj(Map("c" -> acFilter(O.projection(acKey)))),
                O.projection(aKey)))))))))

      pipeEqual(actual, expected)
    }

    "erasing objects check, array check and preserving arrays" >> {
      val actual = evalMask(Map(
        CPath.parse(".a") -> Set(ColumnType.Object),
        CPath.parse(".a.b") -> Set(ColumnType.Boolean),
        CPath.parse(".c") -> Set(ColumnType.Array),
        CPath.parse(".c[1]") -> Set(ColumnType.Number, ColumnType.String),
        CPath.parse(".d[1]") -> Set(ColumnType.Boolean)))

      val aKey = Projection.key("root") + Projection.key("a")
      val cKey = Projection.key("root") + Projection.key("c")
      val dKey = Projection.key("root") + Projection.key("d")
      val d1Key = dKey + Projection.index(1)
      val expected = mkExpected(Map(
        "root" -> O.$cond(
          O.$or(List(
            typeEq(O.projection(cKey), "array"),
            typeEq(O.projection(d1Key), "bool"),
            typeEq(O.projection(aKey), "object"))),
          O.$cond(
            typeEq(rootKey, "object"),
            O.obj(Map(
              "c" ->
                O.$cond(
                  typeEq(O.projection(cKey), "array"),
                  O.projection(cKey),
                  undefined),
              "d" ->
                O.$cond(
                  typeEq(O.projection(d1Key), "bool"),
                  O.$cond(
                    typeEq(O.projection(dKey), "array"),
                    O.array(List(
                      O.$cond(
                        typeEq(O.projection(d1Key), "bool"),
                        O.projection(d1Key),
                        undefined))),
                    undefined),
                  undefined),
              "a" ->
                O.$cond(
                  typeEq(O.projection(aKey), "object"),
                  O.projection(aKey),
                  undefined))),
            undefined),
          undefined)))

      pipeEqual(actual, expected)
    }
  }


}
