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

import quasar.{RenderTree, RenderedTree}, RenderTree.ops._
import quasar.contrib.iota._

import cats.{Functor, ~>}
import cats.data.Const
import higherkindness.droste.{Algebra, Basis, Delay, scheme}
import higherkindness.droste.data.Fix
import iota.{CopK, TListK, TNilK}, TListK.:::

package object expression {
  type CoreL = Core ::: TNilK
  type CoreOpL = Core ::: Op ::: TNilK
  type ExprL = Core ::: Op ::: Const[Projection, ?] ::: TNilK
  type Core0[A] = CopK[CoreL, A]
  type CoreOp[A] = CopK[CoreOpL, A]
  type Expr[A] = CopK[ExprL, A]

  def missing[F[a] <: ACopK[a], U](k: String)(implicit I: Core :<<: F, P: Basis[F, U]): U =
    Optics.core(Optics.basisPrism[F, U]).str(k.concat(MissingSuffix))

  def missingKey[F[a] <: ACopK[a], U](key: String)(implicit I: Core :<<: F, U: Basis[F, U]): U =
    Optics.core(Optics.basisPrism[F, U]).str("$".concat(key).concat(MissingSuffix))

  val MissingSuffix = "_missing"
}
