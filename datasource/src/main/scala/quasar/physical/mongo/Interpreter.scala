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

import cats.effect.Sync
import cats.syntax.functor._

import org.bson.BsonValue

import quasar.ParseInstruction
import quasar.IdStatus
import quasar.physical.mongo.interpreter._

final case class Interpretation(
  remainingInstructions: List[ParseInstruction],
  aggregators: List[Aggregator])

object Interpretation {
  def initial(inst: List[ParseInstruction]): Interpretation = Interpretation(inst, List())
}

trait Interpreter {
  def interpret(idStatus: IdStatus, inp: List[ParseInstruction]): Interpretation
  def refineInterpretation(key: String, interpretation: Interpretation): Interpretation
  def mapper(x: BsonValue): BsonValue
}

class ParseInstructionInterpreter(version: Version, uniqueKey: String) extends Interpreter {
  private val E = MongoExpression
  def mapper(x: BsonValue): BsonValue =
    x.asDocument().get(uniqueKey)

  @scala.annotation.tailrec
  private def refineInterpretationImpl(key: String, interpretation: Interpretation): Interpretation =
    interpretation.remainingInstructions match {
      case List() => interpretation
      case hd :: tail => refineStep(key, hd) match {
        case None => interpretation
        case Some(aggregators) => refineInterpretationImpl(key, Interpretation(tail, interpretation.aggregators ++ aggregators))
      }
    }

  def refineInterpretation(key: String, interpretation: Interpretation): Interpretation =
    refineInterpretationImpl(key, interpretation)

  private def refineStep(key: String, instruction: ParseInstruction): Option[List[Aggregator]] = instruction match {
    case ParseInstruction.Wrap(name) => Wrap(key, version, name)
    case ParseInstruction.Mask(masks) => Mask(key, version, masks)
    case ParseInstruction.Pivot(status, structure) => Pivot(key, version, status, structure)
    case ParseInstruction.Project(path) => Project(key, version, path)
    case ParseInstruction.Cartesian(cartouches) => Cartesian(key, version, cartouches, this)
  }

  private def initialAggregators(idStatus: IdStatus): Aggregator = idStatus match {
    case IdStatus.IdOnly => Aggregator.project(E.Object(uniqueKey -> E.key("_id")))
    case IdStatus.ExcludeId => Aggregator.project(E.Object(uniqueKey -> E.key("$$ROOT")))
    case IdStatus.IncludeId => Aggregator.project(E.Object(uniqueKey -> E.Array(E.key("_id"), E.key("$$ROOT"))))
  }

  def interpret(idStatus: IdStatus, inp: List[ParseInstruction]): Interpretation =
    refineInterpretation(uniqueKey, Interpretation(inp, List(initialAggregators(idStatus))))
}

object ParseInstructionInterpreter {
  def apply[F[_]: Sync](version: Version): F[ParseInstructionInterpreter] =
    Sync[F].delay(java.util.UUID.randomUUID().toString) map (new ParseInstructionInterpreter(version, _))
}
