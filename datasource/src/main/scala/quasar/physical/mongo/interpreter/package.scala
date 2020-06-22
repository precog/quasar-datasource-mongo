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

import quasar.physical.mongo.expression.Mapper

import cats.{~>, MonoidK, Applicative}
import cats.data.StateT
import cats.implicits._
import cats.mtl.MonadState

package object interpreter {
  type InState[A] = StateT[Option, InterpretationState, A]

  type MonadInState[F[_]] = MonadState[F, InterpretationState]

  def focus[F[_]: MonadInState]: F[Unit] =
    MonadState[F, InterpretationState].modify { x => x.copy(mapper = Mapper.Focus(x.uniqueKey)) }

  def unfocus[F[_]: MonadInState]: F[Unit] =
    MonadState[F, InterpretationState].modify { x => x.copy(mapper = Mapper.Unfocus) }

  def optToAlternative[F[_]: MonoidK: Applicative]: Option ~> F = new (Option ~> F) {
    def apply[A](inp: Option[A]): F[A] = inp match {
      case None => MonoidK[F].empty
      case Some(a) => a.pure[F]
    }
  }
}
