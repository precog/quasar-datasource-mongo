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

package quasar.physical.mongo.expression

import slamdata.Predef._

import matryoshka._

import monocle.{Prism, Iso}

import quasar.fp.PrismNT

import scalaz.{:<:, Const, Functor}

object Optics {
  class CoreOptics[A, O, F[_]] private[Optics] (basePrism: Prism[O, F[A]])(implicit c: Core :<: F) extends {
    val corePrism = basePrism composePrism PrismNT.inject[Core, F].asPrism[A]
  } with Core.Optics[A, O]

  class CoreOpOptics[A, O, F[_]] private[Optics] (basePrism: Prism[O, F[A]])(
      implicit c: Core :<: F, o: Op :<: F) extends {
    val corePrism = basePrism composePrism PrismNT.inject[Core, F].asPrism[A]
    val opPrism = basePrism composePrism PrismNT.inject[Op, F].asPrism[A]
  } with Core.Optics[A, O] with Op.Optics[A, O]

  class FullOptics[A, O, F[_]] private[Optics] (basePrism: Prism[O, F[A]])(
      implicit c: Core :<: F, o: Op :<: F, p: Const[Projection, ?] :<: F) extends {
    val corePrism = basePrism composePrism PrismNT.inject[Core, F].asPrism[A]
    val opPrism = basePrism composePrism PrismNT.inject[Op, F].asPrism[A]
    val prjPrism = basePrism composePrism PrismNT.inject[Const[Projection, ?], F].asPrism[A]
  }  with Core.Optics[A, O] with Op.Optics[A, O] {
    val _steps: Iso[Const[Projection, A], List[Step]] =
      Iso[Const[Projection, A], List[Step]] {
        case Const(Projection(steps)) => steps
      } { steps => Const(Projection(steps)) }

    val steps: Prism[O, List[Step]] =
      prjPrism composeIso _steps

    val _projection: Iso[Const[Projection, A], Projection] =
      Iso[Const[Projection, A], Projection] {
        case Const(p) => p
      } { p => Const(p) }

    val projection: Prism[O, Projection] =
      prjPrism composeIso _projection

    val key: Prism[O, String] =
      steps composePrism {
        Prism.partial[List[Step], String] {
          case Field(s) :: List() => s
        } { s => List(Field(s)) }
      }
  }

  def core[A, O, F[_]](basePrism: Prism[O, F[A]])(implicit c: Core :<: F): CoreOptics[A, O, F] =
    new CoreOptics(basePrism)

  def coreOp[A, O, F[_]](basePrism: Prism[O, F[A]])(implicit c: Core :<: F, o: Op :<: F): CoreOpOptics[A, O, F] =
    new CoreOpOptics(basePrism)

  def full[A, O, F[_]](basePrism: Prism[O, F[A]])(
      implicit c: Core :<: F,
      o: Op :<: F,
      p: Const[Projection, ?] :<: F)
      : FullOptics[A, O, F] = {
    new FullOptics(basePrism)
  }

  def fullT[T[_[_]], F[_]](
      implicit c: Core :<: F,
      o: Op :<: F,
      p: Const[Projection, ?] :<: F,
      f: Functor[F],
      t: BirecursiveT[T])
      : FullOptics[T[F], T[F], F] =
    new FullOptics(birecursiveIso[T[F], F].reverse.asPrism)
}
