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

import scalaz.{State, Scalaz}, Scalaz._

class ProjectSpec extends Specification with quasar.TreeMatchers {
  "unfocused" >> {
    val foobarPrj = Projection.key("foo") + Projection.key("bar")
    val actual = Project[State[InterpretationState, ?]](foobarPrj) run InterpretationState("root", Mapper.Unfocus)
    val expected = List(
      Pipeline.$match(Map("foo.bar" -> O.$exists(O.bool(true)))),
      Pipeline.$project(Map("root_project" -> O.projection(foobarPrj))),
      Pipeline.$project(Map("root" -> O.string("$root_project"))))
    (actual._2 must beTree(expected)) and (actual._1.mapper === Mapper.Focus("root"))
  }
  "focused" >> {
    val foo1Prj = Projection.key("foo") + Projection.index(1)
    val actual = Project[State[InterpretationState, ?]](foo1Prj) run InterpretationState("other", Mapper.Focus("other"))
    val expected = List(
      Pipeline.$match(Map("other.foo.1" -> O.$exists(O.bool(true)))),
      Pipeline.$project(Map("other_project" -> O.projection(Projection.key("other") + foo1Prj))),
      Pipeline.$project(Map("other" -> O.string("$other_project"))))
    (actual._2 must beTree(expected)) and (actual._1.mapper === Mapper.Focus("other"))
  }
}
