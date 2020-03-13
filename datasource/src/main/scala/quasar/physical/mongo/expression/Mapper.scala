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

package quasar.physical.mongo.expression

import slamdata.Predef._

import org.bson._

trait Mapper extends Product with Serializable

import scalaz.{Order, Ordering, Scalaz}, Scalaz._

object Mapper {
  final case class Focus(str: String) extends Mapper
  final case object Unfocus extends Mapper

  def bson(mapper: Mapper)(inp: BsonValue): BsonValue = mapper match {
    case Focus(str) => inp.asDocument().get(str)
    case Unfocus => inp
  }

  def projection(mapper: Mapper)(inp: Projection): Projection = mapper match {
    case Focus(str) => Projection.key(str) + inp
    case Unfocus => inp
  }

  implicit val orderMapper: Order[Mapper] = new Order[Mapper] {
    def order(a: Mapper, b: Mapper) = a match {
      case Unfocus => b match {
        case Unfocus => Ordering.EQ
        case Focus(_) => Ordering.LT
      }
      case Focus(afld) => b match {
        case Unfocus => Ordering.GT
        case Focus(bfld) => Order[String].order(afld, bfld)
      }
    }
  }
}
