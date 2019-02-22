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
import cats.syntax.order._

import org.bson.BsonValue

import quasar.{ScalarStage, ScalarStages}
import quasar.IdStatus
import quasar.physical.mongo.interpreter._

final case class Interpretation(
  stages: List[ScalarStage],
  aggregators: List[Aggregator])

object Interpretation {
  def initial(inst: List[ScalarStage]): Interpretation = Interpretation(inst, List())
}

trait Interpreter {
  def interpret(stages: ScalarStages): Interpretation
  def refineInterpretation(key: String, interpretation: Interpretation): Interpretation
  def mapper(x: BsonValue): BsonValue
}

class MongoInterpreter(version: Version, uniqueKey: String, pushdownLevel: PushdownLevel) extends Interpreter {
  private val E = MongoExpression
  def mapper(x: BsonValue): BsonValue =
    x.asDocument().get(uniqueKey)

  @scala.annotation.tailrec
  private def refineInterpretationImpl(key: String, interpretation: Interpretation): Interpretation =
    interpretation.stages match {
      case List() =>
        interpretation
      case hd :: tail => refineStep(key, hd) match {
        case None => interpretation
        case Some(aggregators) => refineInterpretationImpl(key, Interpretation(tail, interpretation.aggregators ++ aggregators))
      }
    }

  def refineInterpretation(key: String, interpretation: Interpretation): Interpretation =
    refineInterpretationImpl(key, interpretation)

  private def refineStep(key: String, instruction: ScalarStage): Option[List[Aggregator]] = instruction match {
    case ScalarStage.Wrap(name) =>
      if (pushdownLevel < PushdownLevel.Light) None
      else E.safeField(name) map { wrap => Wrap(key, version, wrap) }
    case ScalarStage.Mask(masks) =>
      if (pushdownLevel < PushdownLevel.Light) None else Mask(key, version, masks)
    case ScalarStage.Pivot(status, structure) =>
      if (pushdownLevel < PushdownLevel.Light) None else Pivot(key, version, status, structure)
    case ScalarStage.Project(path) =>
      if (pushdownLevel < PushdownLevel.Light) None else Project(key, version, path)
    case ScalarStage.Cartesian(cartouches) =>
      if (pushdownLevel < PushdownLevel.Full) None
      else E.safeCartouches(cartouches) flatMap { x => Cartesian(key, version, x, this) }

  }

  private def initialAggregators(idStatus: IdStatus): Aggregator = idStatus match {
    case IdStatus.IdOnly => Aggregator.project(E.Object(
      uniqueKey -> E.key("_id"),
      "_id" -> E.Int(0)))
    case IdStatus.ExcludeId => Aggregator.project(E.Object(
      uniqueKey -> E.Projection(),
      "_id" -> E.Int(0)))
    case IdStatus.IncludeId => Aggregator.project(E.Object(
      uniqueKey -> E.Array(E.key("_id"), E.Projection()),
      "_id" -> E.Int(0)))
  }

  def interpret(stages: ScalarStages): Interpretation =
    refineInterpretation(uniqueKey, Interpretation(stages.stages, List(initialAggregators(stages.idStatus))))
}

object MongoInterpreter {
  def apply[F[_]: Sync](version: Version, pushdownLevel: PushdownLevel): F[MongoInterpreter] =
    Sync[F].delay(java.util.UUID.randomUUID().toString) map (new MongoInterpreter(version, _, pushdownLevel))
}
