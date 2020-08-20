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

import quasar.api.push.OffsetKey
import quasar.{ScalarStage, ScalarStages, IdStatus}
import quasar.connector.Offset
import quasar.physical.mongo.expression._
import quasar.physical.mongo.interpreter._

import cats.{Applicative, Monad, MonoidK, Id}
import cats.implicits._
import cats.mtl.implicits._
import higherkindness.droste.Basis
import higherkindness.droste.data.Fix

import org.bson._

import skolems.∃

trait Interpreter[F[_], U, -S] {
  def o(implicit U: Basis[Expr, U]) = Optics.full(Optics.basisPrism[Expr, U])
  def apply(s: S): F[List[Pipeline[U]]]
}

object Interpreter {

  final case class Interpretation(stages: List[ScalarStage], docs: List[BsonDocument])

  type StageInterpret = (ScalarStages, Option[Offset]) => (Interpretation, Mapper)
  type OffsetInterpret = Offset => Option[List[BsonDocument]]

  def stages(version: Version, pushdown: PushdownLevel, uuid: String): StageInterpret = { (stages, offset) =>
    val stage = interpretStage[InState, Fix[Expr]](pushdown)
    val idStatus = interpretIdStatus[InState, Fix[Expr]](uuid)
    val offsetDocs = offset.toList.flatMap(interpretOffset[Fix[Expr]].apply(_))

    def refine(inp: Interpretation): InState[Either[Interpretation, Interpretation]] =
      inp.stages match {
        case List() => inp.asRight[Interpretation].pure[InState]

        case hd :: tail =>
          val nextStep = for {
            pipes <- stage.apply(hd)
            docs <- Compiler.compile[InState, Fix[Expr]](version, uuid, pipes)
          } yield Interpretation(tail, inp.docs ++ docs).asLeft[Interpretation]

          nextStep <+> inp.asRight.pure[InState]
      }

    val interpreted = for {
      initPipes <- idStatus(stages.idStatus)
      initDocs <- Compiler.compile[InState, Fix[Expr]](version, uuid, offsetDocs ++ initPipes)
      refined <- Monad[InState].tailRecM(Interpretation(stages.stages, initDocs))(refine)
    } yield refined

    interpreted.run(InterpretationState(uuid, Mapper.Unfocus)) match {
      case None =>
        (Interpretation(stages.stages, List()), Mapper.Unfocus)
      case Some((state, a)) =>
        (a, state.mapper)
    }
  }

  def offset(version: Version, uuid: String): OffsetInterpret =
    offset => Compiler.compile[Option, Fix[Expr]](version, uuid, interpretOffset.apply(offset))

  def interpretOffset[U: Basis[Expr, ?]]
      : Interpreter[Id, U, Offset] =
    new Interpreter[Id, U, Offset] {
      def apply(offset: Offset): List[Pipeline[U]] = {
        val path = offset.path
        val key = offset.value

        val path0 = path map {
          case Left(field) => field
          case Right(right) => right.show
        }

        val value = key match {
          case ∃(offsetKey : OffsetKey.RealKey[Id]) =>
            if (offsetKey.value.isValidInt)
              o.int(offsetKey.value.intValue)
            else if (offsetKey.value.isValidLong)
              o.long(offsetKey.value.longValue)
            else
              o.double(offsetKey.value.doubleValue)

          case ∃(offsetKey : OffsetKey.StringKey[Id]) =>
            o.str(offsetKey.value)

          case ∃(offsetKey : OffsetKey.DateTimeKey[Id]) =>
            o.dateTime(offsetKey.value)
        }

        val filterField = path0.intercalate(".")

        List(
          Pipeline.Match(o.obj(Map(filterField -> o.gte(value)))))
      }
    }

  def interpretStage[F[_]: Monad: MonadInState: MonoidK, U: Basis[Expr, ?]](pushdown: PushdownLevel)
      : Interpreter[F, U, ScalarStage] =
    new Interpreter[F, U, ScalarStage] {
      def apply(s: ScalarStage) =
        if (pushdown < PushdownLevel.Light)
          MonoidK[F].empty[List[Pipeline[U]]]
        else s match {
          case a: ScalarStage.Wrap => Wrap[F, U].apply(a)
          case a: ScalarStage.Mask => Mask[F, U].apply(a)
          case a: ScalarStage.Pivot => Pivot[F, U].apply(a)
          case a: ScalarStage.Project => Project[F, U].apply(a)
          case _: ScalarStage.Cartesian if pushdown < PushdownLevel.Full => MonoidK[F].empty[List[Pipeline[U]]]
          case a: ScalarStage.Cartesian => Cartesian[F, U](interpretStage[F, U](pushdown)).apply(a)
        }
    }

  ////


  private def interpretIdStatus[F[_]: Applicative: MonadInState, U: Basis[Expr, ?]](uq: String)
      : Interpreter[F, U, IdStatus] =
    new Interpreter[F, U, IdStatus] {
      def apply(s: IdStatus) = s match {
        case IdStatus.IdOnly =>
          focus[F] as List(Pipeline.Project(Map(
            uq -> o.str("$_id"),
            "_id" -> o.int(0))))

        case IdStatus.ExcludeId =>
          List[Pipeline[U]]().pure[F]

        case IdStatus.IncludeId =>
          focus[F] as List(Pipeline.Project(Map(
            uq -> o.array(List(o.str("$_id"), o.steps(List()))),
            "_id" -> o.int(0))))
      }
    }
}
