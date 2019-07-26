/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.physical.mongo

import slamdata.Predef._

import cats.kernel.Order
import cats.syntax.eq._
import cats.instances.int._

import scalaz.{Order => Zorder, Ordering}

final case class Version(major: Int, minor: Int, patch: Int)

object Version {
  implicit val orderVersion: Order[Version] = new Order[Version] {
    def compare(a: Version, b: Version) = {
      val major = a.major compare b.major
      if (major =!= 0) major
      else {
        val minor = a.minor compare b.minor
        if (minor =!= 0) minor
        else {
          a.patch compare b.patch
        }
      }
    }
  }
  // shims are very slow in functors
  implicit val zorderVersion: Zorder[Version] = new Zorder[Version] {
    def order(a: Version, b: Version) = {
      Ordering.fromInt(orderVersion.compare(a, b))
    }
  }

  val zero: Version = Version(0, 0, 0)
  val $type: Version = Version(3, 4, 0)
  val $reduce: Version = Version(3, 4, 0)
  val $objectToArray: Version = Version(3, 4, 4)
  val $arrayElemAt: Version = Version(3, 2, 0)
  val $unwind: Version = Version(3, 2, 0) // we use includeArrayIndices added in 3.2.0
  val $concatArrays: Version = Version(3, 2, 0)
}
