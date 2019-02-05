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
  aggregators: List[Aggregator])

trait Interpreter {
  def interpret(inp: List[ParseInstruction]): Interpretation
  def mapper(x: BsonValue): BsonValue
}

class ParseInstructionInterpreter(version: Version, uniqueKey: String) extends Interpreter {
  def mapper(x: BsonValue): BsonValue =
    x.asDocument().get(uniqueKey)

  @scala.annotation.tailrec
  private def interpretImpl(processed: List[ParseInstruction], interpretation: Interpretation): Interpretation =
    interpretation.remainingInstructions match {
      case List() => interpretation
      case hd :: tail => interpretStep(hd, processed) match {
        case None => interpretation
        case Some(aggregators) => interpretImpl(
          hd :: processed,
          Interpretation(tail, interpretation.aggregators ++ aggregators))
      }
    }

  def interpretStep(
      instruction: ParseInstruction,
      processed: List[ParseInstruction])
      : Option[List[Aggregator]] = instruction match {
    case ParseInstruction.Ids => Ids(uniqueKey, version, processed)
    case ParseInstruction.Wrap(path, name) => Wrap(uniqueKey, version, processed, path, name)
    case ParseInstruction.Mask(masks) => Mask(uniqueKey, version, processed, masks)
    case ParseInstruction.Pivot(path, status, structure) => Pivot(uniqueKey, version, path, status, structure)
    case ParseInstruction.Project(path) => Project(uniqueKey, version, processed, path)
    case ParseInstruction.Cartesian(cartouches) => None
  }

  private val initialAggregators: List[Aggregator] =
    List(Aggregator.project(MongoExpression.MongoObject(Map(
      uniqueKey -> MongoProjection.Root))))

  def interpret(inp: List[ParseInstruction]): Interpretation =
    interpretImpl(List(), Interpretation(inp, initialAggregators))
}

object ParseInstructionInterpreter {
  def apply[F[_]: Sync](version: Version): F[ParseInstructionInterpreter] =
    Sync[F].delay(java.util.UUID.randomUUID().toString) map (new ParseInstructionInterpreter(version, _))
}
