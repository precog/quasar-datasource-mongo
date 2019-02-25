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
import org.specs2.ScalaCheck

import quasar.physical.mongo.Version
import quasar.physical.mongo.expression._

class WrapSpec extends Specification with ScalaCheck {
  "wrap interpreter produces two projects" >> prop { (wrapString: String, version: Version, rootKey: String) =>
    ((wrapString.length > 0) && (wrapString != "\u0000")) ==> {
      val interpretation = Wrap.apply0(rootKey, Field(wrapString))
      interpretation must beLike {
        case Pipeline.$project(prj1) :: Pipeline.$project(prj2) :: List() =>
          (prj1.get(rootKey concat "_wrap") === Some(O.obj(Map(wrapString -> O.key(rootKey))))) and
          (prj2.get(rootKey) === Some(O.key(rootKey concat "_wrap")))
      }
    }
  }
  "wrap example" >> {
    val actual = Wrap.apply0("root", Field("wrapper"))
    val expected = List(
      Pipeline.$project(Map("root_wrap" -> O.obj(Map("wrapper" -> O.key("root"))))),
      Pipeline.$project(Map("root" -> O.key("root_wrap"))))
    actual === expected
  }
}
