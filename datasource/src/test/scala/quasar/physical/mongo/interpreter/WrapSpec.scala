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

import quasar.physical.mongo.{Version, Aggregator, MongoExpression => E}

class WrapSpec extends Specification with ScalaCheck {
  "wrap interpreter produces two projects" >> prop { (wrapString: String, version: Version, rootKey: String) =>
    ((wrapString.length > 0) && (wrapString != "\u0000")) ==> {
      val interpretation = Wrap(rootKey, version, wrapString)
      interpretation must beLike {
        case Some(Aggregator.project(E.Object(firstPair)) :: Aggregator.project(E.Object(secondPair)) :: List()) =>
          (secondPair._1 === rootKey) and
          (secondPair._2 === E.key(firstPair._1)) and
          (firstPair._2 === E.Object(wrapString -> E.key(rootKey)))
      }
    }
  }
  "wrap example" >> {
    val actual = Wrap("root", Version.zero, "wrapper")
    val expected = Some(List(
      Aggregator.project(E.Object("root_wrap" -> E.Object("wrapper" -> E.key("root")))),
      Aggregator.project(E.Object("root" -> E.key("root_wrap")))))
    actual === expected
  }
}
