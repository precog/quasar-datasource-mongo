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

package quasar.physical.mongo.interpreter

import slamdata.Predef._

import quasar.physical.mongo.expression._

import scalaz.{MonadState, Scalaz}, Scalaz._

object Project {
  def apply[F[_]: MonadInState](fld: Projection): F[List[Pipe]] = for {
    state <- MonadState[F, InterpretationState].get
    tmpKey = state.uniqueKey concat "_project"
    res = List(
      Pipeline.$project(Map(tmpKey -> O.projection(fld))),
      Pipeline.$project(Map(state.uniqueKey -> O.string("$" concat tmpKey))))
    _ <- focus[F]
  } yield res map mapProjection(state.mapper)
}
