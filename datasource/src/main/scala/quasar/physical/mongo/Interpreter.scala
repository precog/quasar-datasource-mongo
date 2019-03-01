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

import org.bson.{BsonValue, BsonDocument}

import quasar.{ScalarStage, ScalarStages}
import quasar.IdStatus
import quasar.physical.mongo.interpreter.{InterpretationState, _}
import quasar.physical.mongo.expression._
import quasar.physical.mongo.utils._

import scalaz.{StateT, BindRec, \/, Scalaz, PlusEmpty, Applicative}, Scalaz._

import shims._

final case class Interpretation(
  stages: List[ScalarStage],
  docs: List[BsonDocument])

class Interpreter(version: Version, uniqueKey: String, pushdownLevel: PushdownLevel) {
  def mapper(x: BsonValue): BsonValue =
    x.asDocument().get(uniqueKey)

  private def refine(inp: Interpretation)
      : InState[Interpretation \/ Interpretation] = inp.stages match {
    case List() => inp.right[Interpretation].point[InState]
    case hd :: tail =>
      val nextStep = for {
        pipes <- interpretStep[InState](hd)
        docs <- compilePipeline[InState](version, pipes)
      } yield Interpretation(tail, inp.docs ++ docs).left[Interpretation]
      nextStep <+> inp.right[Interpretation].point[InState]
  }

  def interpretStep[F[_]: MonadInState: PlusEmpty](instruction: ScalarStage): F[List[Pipe]] = instruction match {
    case ScalarStage.Wrap(name) =>
      optToAlternative[F].apply(Projection.safeField(name)) flatMap (Wrap[F](_))
    case ScalarStage.Mask(masks) =>
      Mask[F](masks)
    case ScalarStage.Pivot(status, structure) =>
      Pivot[F](status, structure)
    case ScalarStage.Project(path) =>
      optToAlternative[F].apply(Projection.fromCPath(path)) flatMap (Project[F](_))
    case ScalarStage.Cartesian(cartouches) =>
      if (pushdownLevel < PushdownLevel.Full) PlusEmpty[F].empty
      else optToAlternative[F].apply(Projection.safeCartouches(cartouches)) flatMap (Cartesian[F](_, interpretStep[F]))
  }

  private def initial[F[_]: MonadInState](idStatus: IdStatus): F[List[Pipe]] = idStatus match {
    case IdStatus.IdOnly =>
      nest[F] as List(Pipeline.$project(Map(
        uniqueKey -> O.key("_id"),
        "_id" -> O.int(0))))
    case IdStatus.ExcludeId =>
      List[Pipe]().point[F]
    case IdStatus.IncludeId =>
      nest[F] as List(Pipeline.$project(Map(
        uniqueKey -> O.array(List(O.key("_id"), O.steps(List()))),
        "_id" -> O.int(0))))
  }

  def interpret(stages: ScalarStages): (Interpretation, Mapper) = {
    val interpreted = for {
      firstPipes <- initial[InState](stages.idStatus)
      firstDocs <- firstPipes.traverse (compilePipe[InState](version, _))
      result <- BindRec[InState].tailrecM(refine)(Interpretation(stages.stages, firstDocs))
    } yield result
    interpreted.run(InterpretationState(uniqueKey, Mapper.Identity)) match {
      case None =>
        (Interpretation(stages.stages, List()), Mapper.Identity)
      case Some((state, a)) => (a, state.mapper)
    }
  }
}

object Interpreter {
  def apply[F[_]: Sync](version: Version, pushdownLevel: PushdownLevel): F[Interpreter] =
    Sync[F].delay(java.util.UUID.randomUUID().toString) map (new Interpreter(version, _, pushdownLevel))
}
