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

package quasar.physical.mongo.expression

import quasar.contrib.iotac._

import cats.data.Const
import higherkindness.droste._
import higherkindness.droste.data._
import higherkindness.droste.syntax.all._
import monocle.{Iso, Prism}

object Optics {
  def copkPrism[F[_], G[a] <: ACopK[a], A](implicit I: F :<<: G): Prism[G[A], F[A]] =
    Prism[G[A], F[A]]((x: G[A]) => I.prj(x))((x: F[A]) => I.inj(x))

  def basisIso[F[_], U: Basis[F, ?]]: Iso[U, F[U]] =
    Iso((x: U) => x.project)((x: F[U]) => x.embed)

  def basisPrism[F[_], U](implicit basis: Basis[F, U]): Prism[U, F[U]] =
    basisIso.asPrism

  def coattrFPrism[F[_], A, B] =
    Prism.partial[CoattrF[F, A, B], F[B]]{case CoattrF.Roll(f) => f}(CoattrF.roll(_))

  def projection[A, O](bp: Prism[O, Const[Projection, A]]): Projection.Optics[A, O] =
    new { val proj = bp } with Projection.Optics[A, O]

  def core[F[a] <: ACopK[a], A, O](bp: Prism[O, F[A]])(implicit c: Core :<<: F): Core.Optics[A, O] = new {
    val core = bp composePrism copkPrism[Core, F, A]
  } with Core.Optics[A, O]

  def coreOp[F[a] <: ACopK[a], A, O](bp: Prism[O, F[A]])(implicit c: Core :<<: F, o: Op :<<: F): Core.Optics[A, O] with Op.Optics[A, O] =
    new {
      val core = bp composePrism copkPrism[Core, F, A]
      val op = bp composePrism copkPrism[Op, F, A]
    } with Core.Optics[A, O] with Op.Optics[A, O]

  def full[F[a] <: ACopK[a], A, O](bp: Prism[O, F[A]])(
      implicit
      c: Core :<<: F,
      o: Op :<<: F,
      p: Const[Projection, ?] :<<: F)
      : Core.Optics[A, O] with Op.Optics[A, O] with Projection.Optics[A, O] =
  new {
    val core = bp composePrism copkPrism[Core, F, A]
    val op = bp composePrism copkPrism[Op, F, A]
    val proj = bp composePrism copkPrism[Const[Projection, ?], F, A]
  } with Core.Optics[A, O] with Op.Optics[A, O] with Projection.Optics[A, O]

}
