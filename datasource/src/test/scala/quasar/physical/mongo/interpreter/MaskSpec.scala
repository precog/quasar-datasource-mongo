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

import org.specs2.mutable.Specification

import quasar.api.table.ColumnType
import quasar.common.CPath
import quasar.physical.mongo.MongoExpression
import quasar.physical.mongo.{Version, Aggregator, MongoExpression => E}, E.helpers._

class MaskSpec extends Specification {
  "min versions" >> {
    "older than $type" >> {
      Mask("root", Version.zero, Map(CPath.parse(".") -> ColumnType.Top)) must beNone
    }
    "newer than $type" >> {
      Mask("root", Version.$type, Map(CPath.parse(".") -> ColumnType.Top)) must beSome
    }
  }

  "examples" >> {
    def evalMask(masks: Map[CPath, Set[ColumnType]]): Option[List[Aggregator]] =
      Mask("root", Version.$type, masks)

    val rootKey = E.key("root")

    val undefined = E.key("root_non_existent_field")

    def mkExpected(e: MongoExpression) =
      Some(List(Aggregator.project(e), Aggregator.notNull("root")))

    def typeEq(prj: MongoExpression, s: String): MongoExpression =
      equal(typeExpr(prj), E.String(s))

    def isObjectFilter(x: MongoExpression, y: MongoExpression): MongoExpression =
      cond(typeEq(x, "object"), y, undefined)


    "drop everything when empty" >> {
      val actual = evalMask(Map.empty)
      val expected = Some(List(Aggregator.filter(E.Object("root_non_existent_field" -> E.Bool(false)))))
      actual === expected
    }

    "numbers and booleans types at identity" >> {
      val actual = evalMask(Map(CPath.Identity -> Set(ColumnType.Number, ColumnType.Boolean)))

      val expected = mkExpected(
        E.Object("root" -> cond(
          or(List(
            typeEq(rootKey, "double"),
            typeEq(rootKey, "long"),
            typeEq(rootKey, "int"),
            typeEq(rootKey, "decimal"),
            typeEq(rootKey, "bool"))),
          rootKey,
          undefined)))
      actual === expected
    }

    "objects at identity" >> {
      val actual = evalMask(Map(CPath.Identity -> Set(ColumnType.Object)))
      val expected = mkExpected(E.Object("root" -> cond(typeEq(rootKey, "object"), rootKey, undefined)))
      actual === expected
    }

    "mask at path" >> {
      val actual = evalMask(Map(CPath.parse(".a.b") -> Set(ColumnType.String)))
      val aKey = rootKey +/ E.key("a")
      val abKey = aKey +/ E.key("b")

      def abFilter(x: MongoExpression): MongoExpression =
        cond(typeEq(abKey, "string"), x, undefined)

      val expected =
        mkExpected(E.Object(
          "root" -> abFilter(
            isObjectFilter(rootKey, E.Object("a" ->
              abFilter(isObjectFilter(aKey, E.Object("b" ->
                abFilter(abKey)))))))))
      actual === expected
    }

    "composition" >> {
      val actual = evalMask(Map(
        CPath.parse(".a.c") -> Set(ColumnType.Boolean),
        CPath.parse(".a") -> Set(ColumnType.Array)))

      // To me having the keys duplicated here is just coincidence, since they're defined for this very test
      val aKey = rootKey +/ E.key("a")
      val acKey = aKey +/ E.key("c")

      def acFilter(x: MongoExpression) =
        cond(typeEq(acKey, "bool"), x, undefined)

      def bothFilters(x: MongoExpression) =
        cond(or(List(typeEq(aKey, "array"), typeEq(acKey, "bool"))), x, undefined)

      val expected = mkExpected(E.Object(
        "root" -> bothFilters(
          isObjectFilter(rootKey, E.Object("a" ->
            bothFilters(
              cond(
                typeEq(aKey, "object"),
                E.Object("c" -> acFilter(acKey)),
                aKey)))))))

      actual === expected
    }

    "erasing objects check, array check and preserving arrays" >> {
      val actual = evalMask(Map(
        CPath.parse(".a") -> Set(ColumnType.Object),
        CPath.parse(".a.b") -> Set(ColumnType.Boolean),
        CPath.parse(".c") -> Set(ColumnType.Array),
        CPath.parse(".c[1]") -> Set(ColumnType.Number, ColumnType.String),
        CPath.parse(".d[1]") -> Set(ColumnType.Boolean)))

      val aKey = rootKey +/ E.key("a")
      val cKey = rootKey +/ E.key("c")
      val dKey = rootKey +/ E.key("d")
      val d1Key = dKey +/ E.index(1)
      val expected = mkExpected(E.Object(
        "root" -> cond(
          or(List(
            typeEq(cKey, "array"),
            typeEq(d1Key, "bool"),
            typeEq(aKey, "object"))),
          cond(
            typeEq(rootKey, "object"),
            E.Object(
              "c" ->
                cond(
                  typeEq(cKey, "array"),
                  cKey,
                  undefined),
              "d" ->
                cond(
                  typeEq(d1Key, "bool"),
                  cond(
                    typeEq(dKey, "array"),
                    E.Array(
                      cond(
                        typeEq(d1Key, "bool"),
                        d1Key,
                        undefined)),
                    undefined),
                  undefined),
              "a" ->
                cond(
                  typeEq(aKey, "object"),
                  aKey,
                  undefined)),
            undefined),
          undefined)))

      actual === expected
    }
  }


}
