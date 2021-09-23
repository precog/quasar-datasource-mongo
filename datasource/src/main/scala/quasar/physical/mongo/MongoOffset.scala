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

package quasar.physical.mongo

import slamdata.Predef._

import quasar.api.push.InternalKey

import cats.data.NonEmptyList

import skolems.∃

import spire.math.Real

import java.time.OffsetDateTime

/** The subset of offsets currently supported. */
sealed trait MongoOffset extends Product with Serializable {
  def path: NonEmptyList[Either[String, Int]]
}

object MongoOffset {
  final case class RealOffset(path: NonEmptyList[Either[String, Int]], value: Real)
      extends MongoOffset
  final case class StringOffset(path: NonEmptyList[Either[String, Int]], value: String)
      extends MongoOffset
  final case class DateTimeOffset(path: NonEmptyList[Either[String, Int]], value: OffsetDateTime)
      extends MongoOffset

  def apply(path: NonEmptyList[Either[String, Int]], offset: ∃[InternalKey.Actual])
      : Either[String, MongoOffset] = {
    val offset0: InternalKey.Actual[_] = offset.value

    offset0 match {
      case InternalKey.RealKey(n) =>
        Right(RealOffset(path, n))
      case InternalKey.StringKey(n) =>
        Right(StringOffset(path, n))
      case InternalKey.DateTimeKey(n) =>
        Right(DateTimeOffset(path, n))
      case other =>
        Left(s"MongoDB doesn't support $other offset")
    }
  }
}
