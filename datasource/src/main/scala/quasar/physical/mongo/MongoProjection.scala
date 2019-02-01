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

package quasar.physical.mongo

import slamdata.Predef._

trait MongoProjection {
  def toVarString: String
}

object MongoProjection {
  trait SimpleProjection extends MongoProjection

  final case class Field(field: String) extends SimpleProjection {
    def toVarString = "$" ++ field
  }
  final case class Index(index: Int) extends SimpleProjection {
    def toVarString = "$" ++ index.toString
  }

//  final case class Path(steps: List[SimpleProjection]) extends MongoProjection

  val Id: MongoProjection = Field("_id")
  val Root: MongoProjection = Field("$ROOT")
}
