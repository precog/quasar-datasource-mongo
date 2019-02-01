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
import quasar.qscript.InterpretedRead
import quasar.physical.mongo.interpreter._

final case class Interpretation(
  remainingInstructions: List[ParseInstruction],
  aggregators: List[Aggregator],
  resultMapper: (BsonValue => BsonValue))

object Interpretation {
}

trait Interpreter {
  def interpret(inp: List[ParseInstruction]): Interpretation
}

class ParseInstructionInterpreter(version: Version, uniqueKey: String) extends Interpreter {
  private def interpretImpl(processed: List[ParseInstruction], interpretation: Interpretation): Interpretation =
    interpretation.remainingInstructions match {
      case List() => interpretation
      case hd :: tail => interpretStep(hd, processed) match {
        case None => interpretation
        case Some((aggregator, mapper)) => interpretImpl(
          hd :: processed,
          Interpretation(tail, aggregator ++ interpretation.aggregators, mapper))
      }
    }

  def interpretStep(
      instruction: ParseInstruction,
      processed: List[ParseInstruction])
      : Option[(List[Aggregator], BsonValue => BsonValue)] = instruction match {

    case ParseInstruction.Ids => Ids(uniqueKey, version, processed)
    case ParseInstruction.Wrap(path, name) => None
    case ParseInstruction.Mask(masks) => None
    case ParseInstruction.Pivot(path, status, structure) => None
    case ParseInstruction.Project(path) => None
    case ParseInstruction.Cartesian(cartouches) => None
  }

  def interpret(inp: List[ParseInstruction]): Interpretation =
    interpretImpl(List(), Interpretation(inp, List(), (x => x)))
}

object ParseInstructionInterpreter {
  def apply[F[_]: Sync](version: Version): F[ParseInstructionInterpreter] =
    Sync[F].delay(java.util.UUID.randomUUID().toString) map (new ParseInstructionInterpreter(version, _))
}
