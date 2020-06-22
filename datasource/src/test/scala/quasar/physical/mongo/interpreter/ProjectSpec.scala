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

import quasar.ScalarStage
import quasar.contrib.iotac._
import quasar.physical.mongo.expression._
import quasar.physical.mongo.expression.{Projection, Pipeline => P}, Projection._, Step._

import cats.implicits._
import cats.mtl.implicits._
import higherkindness.droste.data.Fix

class ProjectSpec extends Specification with quasar.TreeMatchers {
  val o = Optics.full(Optics.basisPrism[Expr, Fix[Expr]])

  "unfocused" >> {
    val foobarPrj = Projection.key("foo") + Projection.key("bar")
    val interpreter = Project[InState, Fix[Expr]]
    val actual =
      interpreter(ScalarStage.Project(foobarPrj.toCPath))
        .run(InterpretationState("root", Mapper.Unfocus))
    val expected = List(
      P.Project(Map("root_project0" -> o.steps(List()))),
      P.Project(Map("root_project1" -> o.cond(
        o.or(List(
          o.not(o.eqx(List(o.typ(o.key("root_project0")), o.str("object")))),
          o.eqx(List(o.typ(o.steps(List(Field("root_project0"), Field("foo")))), o.str("missing"))))),
        o.str("root_missing"),
        o.steps(List(Field("root_project0"), Field("foo")))))),
      P.Presented,
      P.Project(Map("root_project0" -> o.cond(
        o.or(List(
          o.not(o.eqx(List(o.typ(o.key("root_project1")), o.str("object")))),
          o.eqx(List(o.typ(o.steps(List(Field("root_project1"), Field("bar")))), o.str("missing"))))),
        o.str("root_missing"),
        o.steps(List(Field("root_project1"), Field("bar")))))),
      P.Presented,
      P.Project(Map("root" -> o.key("root_project0"))),
      P.Presented)
    actual must beLike {
      case Some((state, pipes)) =>
        state.mapper must_=== Mapper.Focus("root")
        pipes must beTree(expected)
    }
  }
  "focused" >> {
    val foo1Prj = Projection.key("foo") + Projection.index(1)
    val interpreter = Project[InState, Fix[Expr]]
    val actual =
      interpreter(
        ScalarStage.Project(foo1Prj.toCPath))
        .run(InterpretationState("other", Mapper.Focus("other")))
    val expected = List(
      P.Project(Map("other_project0" -> o.steps(List()))),
      P.Project(Map("other_project1" -> o.cond(
        o.or(List(
          o.not(o.eqx(List(o.typ(o.key("other_project0")), o.str("object")))),
          o.eqx(List(o.typ(o.steps(List(Field("other_project0"), Field("other")))), o.str("missing"))))),
        o.str("other_missing"),
        o.steps(List(Field("other_project0"), Field("other")))))),
      P.Presented,
      P.Project(Map("other_project0" -> o.cond(
        o.or(List(
          o.not(o.eqx(List(o.typ(o.key("other_project1")), o.str("object")))),
          o.eqx(List(o.typ(o.steps(List(Field("other_project1"), Field("foo")))), o.str("missing"))))),
        o.str("other_missing"),
        o.steps(List(Field("other_project1"), Field("foo")))))),
      P.Presented,
      P.Project(Map("other_project1" -> o.cond(
        o.or(List(
          o.not(o.eqx(List(o.typ(o.key("other_project0")), o.str("array")))),
          o.eqx(List(o.typ(o.steps(List(Field("other_project0"), Index(1)))), o.str("missing"))))),
        o.str("other_missing"),
        o.steps(List(Field("other_project0"), Index(1)))))),
      P.Presented,
      P.Project(Map("other" -> o.key("other_project1"))),
      P.Presented)
    actual must beLike {
      case Some((state, pipes)) =>
        state.mapper must_=== Mapper.Focus("other")
        pipes must beTree(expected)
    }
  }
}
