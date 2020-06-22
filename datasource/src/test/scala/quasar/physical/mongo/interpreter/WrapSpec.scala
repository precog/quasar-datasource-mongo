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

import cats.implicits._
import cats.mtl.implicits._
import higherkindness.droste.data.Fix

class WrapSpec extends Specification with quasar.TreeMatchers {
  val o = Optics.full(Optics.basisPrism[Expr, Fix[Expr]])

  "unfocused" >> {
    val interpreter = Wrap[InState, Fix[Expr]]
    val actual = interpreter(ScalarStage.Wrap("wrapper")).run(InterpretationState("root", Mapper.Unfocus))
    val expected = List(
      Pipeline.Project(Map(
        "root" ->
          o.cond(
            o.eqx(List(o.steps(List()), o.str("root_missing"))),
            o.str("root_missing"),
            o.obj(Map("wrapper" -> o.steps(List())))),
        "_id" -> o.int(0))),
      Pipeline.Presented)
    actual must beLike {
      case Some((state, pipes)) =>
        state.mapper must_=== Mapper.Focus("root")
        pipes must beTree(expected)
    }
  }
  "focused" >> {
    val interpreter = Wrap[InState, Fix[Expr]]
    val actual = interpreter(ScalarStage.Wrap("wrapper")).run(InterpretationState("root", Mapper.Focus("root")))
    val expected = List(
      Pipeline.Project(Map(
        "root" ->
          o.cond(
            o.eqx(List(o.key("root"), o.str("root_missing"))),
            o.str("root_missing"),
            o.obj(Map("wrapper" -> o.key("root")))),
        "_id" -> o.int(0))),
      Pipeline.Presented)
    actual must beLike {
      case Some((state, pipes)) =>
        state.mapper must_=== Mapper.Focus("root")
        pipes must beTree(expected)
    }
  }
}
