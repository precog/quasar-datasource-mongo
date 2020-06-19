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

import cats.Order
import cats.implicits._

import org.bson._

import Mapper._

sealed trait Mapper extends Product with Serializable { self =>
  def bson: BsonValue => BsonValue = { inp =>
    self match {
      case Unfocus => inp
      case Focus(str) => inp.asDocument().get(str)
    }
  }
  def projection: Projection => Projection = { inp =>
    self match {
      case Unfocus => inp
      case Focus(str) => Projection.key(str) + inp
    }
  }
}

object Mapper {
  final case class Focus(str: String) extends Mapper
  final case object Unfocus extends Mapper

  implicit val order: Order[Mapper] = new Order[Mapper] {
    def compare(a: Mapper, b: Mapper): Int = a match {
      case Unfocus => b match {
        case Unfocus => 0
        case Focus(_) => -1
      }
      case Focus(afld) => b match {
        case Unfocus => 1
        case Focus(bfld) => Order[String].compare(afld, bfld)
      }
    }
  }
}
