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
import org.specs2.ScalaCheck

import quasar.physical.mongo.expression._

import scalaz.{State, Scalaz}, Scalaz._

class WrapSpec extends Specification with ScalaCheck {
  "unfocused" >> {
    val actual = Wrap[State[InterpretationState, ?]](Field("wrapper")) run InterpretationState("root", Mapper.Unfocus)
    val expected = List(
      Pipeline.$project(Map(
        "root" ->
          O.$cond(
            O.$eq(List(O.steps(List()), O.string("root_missing"))),
            O.string("root_missing"),
            O.obj(Map("wrapper" -> O.steps(List())))),
        "_id" -> O.int(0))),
      Pipeline.Presented)
    (actual._2 === expected) and (actual._1.mapper === Mapper.Focus("root"))
  }
  "focused" >> {
    val actual = Wrap[State[InterpretationState, ?]](Field("wrapper")) run InterpretationState("root", Mapper.Focus("root"))
    val expected = List(
      Pipeline.$project(Map(
        "root" ->
          O.$cond(
            O.$eq(List(O.key("root"), O.string("root_missing"))),
            O.string("root_missing"),
            O.obj(Map("wrapper" -> O.key("root")))),
        "_id" -> O.int(0))),
      Pipeline.Presented)
    (actual._2 === expected) and (actual._1.mapper === Mapper.Focus("root"))
  }
}
