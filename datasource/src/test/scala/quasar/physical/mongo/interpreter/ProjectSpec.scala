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

import cats.syntax.order._

import org.specs2.mutable.Specification

import quasar.physical.mongo.expression._

class ProjectSpec extends Specification {
/*
  "Project examples" >> {
    "old mongo without indices" >> {
      val actual = Project("root", Projection.key("foo") + Projection.key("bar"))
      val expected = List(
        Pipeline.$match(Map("root.foo.bar" -> O.$exists(O.bool(true)))),
        Pipeline.$project(Map("root_project" -> O.projection(
          Projection.key("root") + Projection.key("foo") + Projection.key("bar")))),
        Pipeline.$project(Map("root" -> O.key("root_project"))))
      actual === expected
    }
    "new mongo with indices" >> {
      val actual = Project("other", Projection.key("foo") + Projection.index(1))
      val expected = List(
        Pipeline.$match(Map("other.foo.1" -> O.$exists(O.bool(true)))),
        Pipeline.$project(Map("other_project" -> O.projection(
          Projection.key("other") + Projection.key("foo") + Projection.index(1)))),
        Pipeline.$project(Map("other" -> O.key("other_project"))))
      actual === expected
    }
  }
 */
}
