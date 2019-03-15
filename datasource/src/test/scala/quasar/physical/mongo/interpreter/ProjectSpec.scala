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

import quasar.physical.mongo.expression._
import quasar.physical.mongo.expression.{Projection, Pipeline}, Projection._, Pipeline._

import scalaz.{State, Scalaz}, Scalaz._

class ProjectSpec extends Specification with quasar.TreeMatchers {
  "unfocused" >> {
    val foobarPrj = Projection.key("foo") + Projection.key("bar")
    val actual = Project[State[InterpretationState, ?]](foobarPrj) run InterpretationState("root", Mapper.Unfocus)
    val expected = List(
      $project(Map("root_project0" -> O.steps(List()))),
      $project(Map("root_project1" -> O.$cond(
        O.$or(List(
          O.$not(O.$eq(List(O.$type(O.key("root_project0")), O.string("object")))),
          O.$eq(List(O.$type(O.steps(List(Field("root_project0"), Field("foo")))), O.string("missing"))))),
        O.string("root_missing"),
        O.steps(List(Field("root_project0"), Field("foo")))))),
      Presented,
      $project(Map("root_project0" -> O.$cond(
        O.$or(List(
          O.$not(O.$eq(List(O.$type(O.key("root_project1")), O.string("object")))),
          O.$eq(List(O.$type(O.steps(List(Field("root_project1"), Field("bar")))), O.string("missing"))))),
        O.string("root_missing"),
        O.steps(List(Field("root_project1"), Field("bar")))))),
      Presented,
      $project(Map("root" -> O.key("root_project0"))),
      Presented)
    (actual._2 must beTree(expected)) and (actual._1.mapper === Mapper.Focus("root"))
  }
  "focused" >> {
    val foo1Prj = Projection.key("foo") + Projection.index(1)
    val actual = Project[State[InterpretationState, ?]](foo1Prj) run InterpretationState("other", Mapper.Focus("other"))
    val expected = List(
      $project(Map("other_project0" -> O.steps(List()))),
      $project(Map("other_project1" -> O.$cond(
        O.$or(List(
          O.$not(O.$eq(List(O.$type(O.key("other_project0")), O.string("object")))),
          O.$eq(List(O.$type(O.steps(List(Field("other_project0"), Field("other")))), O.string("missing"))))),
        O.string("other_missing"),
        O.steps(List(Field("other_project0"), Field("other")))))),
      Presented,
      $project(Map("other_project0" -> O.$cond(
        O.$or(List(
          O.$not(O.$eq(List(O.$type(O.key("other_project1")), O.string("object")))),
          O.$eq(List(O.$type(O.steps(List(Field("other_project1"), Field("foo")))), O.string("missing"))))),
        O.string("other_missing"),
        O.steps(List(Field("other_project1"), Field("foo")))))),
      Presented,
      $project(Map("other_project1" -> O.$cond(
        O.$or(List(
          O.$not(O.$eq(List(O.$type(O.key("other_project0")), O.string("array")))),
          O.$eq(List(O.$type(O.steps(List(Field("other_project0"), Index(1)))), O.string("missing"))))),
        O.string("other_missing"),
        O.steps(List(Field("other_project0"), Index(1)))))),
      Presented,
      $project(Map("other" -> O.key("other_project1"))),
      Presented)
    (actual._2 must beTree(expected)) and (actual._1.mapper === Mapper.Focus("other"))
  }
}
