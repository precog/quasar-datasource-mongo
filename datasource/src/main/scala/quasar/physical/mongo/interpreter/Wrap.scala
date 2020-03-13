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

package quasar.physical.mongo.interpreter

import slamdata.Predef._

import quasar.physical.mongo.expression._

import scalaz.{MonadState, Scalaz}, Scalaz._

object Wrap {
  def apply[F[_]: MonadInState](name: Field): F[List[Pipe]] = for {
    state <- MonadState[F, InterpretationState].get
    res = List(Pipeline.$project(Map(
      state.uniqueKey ->
        O.$cond(
          O.$eq(List(O.steps(List()), missing(state.uniqueKey))),
          missing(state.uniqueKey),
          O.obj(Map(name.name -> O.steps(List())))),
      "_id" -> O.int(0))),
      Pipeline.Presented)
    _ <- focus[F]
  } yield res map mapProjection(state.mapper)
}
