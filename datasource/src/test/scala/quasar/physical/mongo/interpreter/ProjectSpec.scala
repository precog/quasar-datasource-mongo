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
import org.specs2.ScalaCheck

import quasar.common.{CPath, CPathField, CPathIndex}
import quasar.physical.mongo.{Version, Aggregator, MongoExpression => E}

class ProjectSpec extends Specification with ScalaCheck {
  import org.scalacheck._
  import Arbitrary._
  implicit def arbitraryCPath: Arbitrary[CPath] = Arbitrary {
    Gen.listOfN(6, Gen.oneOf(Gen.alphaStr.map(CPathField(_)), arbitrary[Int].map(CPathIndex(_)))).map(CPath(_))
  }

  def cpathHasIndices(cpath: CPath): Boolean = cpath.nodes.exists {
    case CPathIndex(_) => true
    case _ => false
  }

  "Project props" >> {
    "should be interpreted to None when version is too small" >> {
      prop { (cpath: CPath, version: Version, rootKey: String) =>
        (cpathHasIndices(cpath) && version < Version.$arrayElemAt) ==> {
          Project(rootKey, version, cpath) must beNone
        }
      }.set(minTestsOk = 1000)
    }
    "should emit Some(exists, tmpKeyProject, backProject) otherwise" >> {
      prop { (cpath: CPath, version: Version, rootKey: String) =>
        val fld = E.cpathToProjection(cpath) getOrElse (E.Projection())
        (fld.minVersion <= version) ==> {
          Project(rootKey, version, cpath) must beLike {
            case Some(actualMatch :: Aggregator.project(E.Object(tmpPair)) :: Aggregator.project(E.Object(backPair)) :: List()) => {
              val projection = E.key(rootKey) +/ fld
              val expectedMatch =
                Aggregator.filter(E.Object(projection.toKey -> E.Object("$exists" -> E.Bool(true))))
              (actualMatch === expectedMatch) and
              (tmpPair._2 === projection) and
              (backPair._2 === E.key(tmpPair._1)) and
              (backPair._1 === rootKey)
            }
          }
        }
      }.set(minTestsOk = 1000)
    }
  }

  "Project examples" >> {
    "too old mongo" >> {
      Project("foo", Version.zero, CPath.parse("foo[0]")) must beNone
    }
    "old mongo without indices" >> {
      val actual = Project("root", Version.zero, CPath.parse("foo.bar"))
      val expected = Some(List(
        Aggregator.filter(E.Object("root.foo.bar" -> E.Object("$exists" -> E.Bool(true)))),
        Aggregator.project(E.Object("root_project" -> (E.key("root") +/ E.key("foo") +/ E.key("bar")))),
        Aggregator.project(E.Object("root" -> E.key("root_project")))))
      actual === expected
    }
    "new mongo with indices" >> {
      val actual = Project("other", Version.$arrayElemAt, CPath.parse("foo[1]"))
      val expected = Some(List(
        Aggregator.filter(E.Object("other.foo.1" -> E.Object("$exists" -> E.Bool(true)))),
        Aggregator.project(E.Object("other_project" -> (E.key("other") +/ E.key("foo") +/ E.index(1)))),
        Aggregator.project(E.Object("other" -> E.key("other_project")))))
      actual === expected
    }
  }

}
