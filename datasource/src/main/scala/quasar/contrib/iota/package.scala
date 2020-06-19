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

package quasar.contrib

import slamdata.Predef.{Eq => _, _}

import quasar.RenderTree

import _root_.iota.TListK.:::
import _root_.iota.{CopK, TListK, TNilK}
import _root_.cats._
import _root_.cats.implicits._

import higherkindness.droste.Delay
import higherkindness.droste.util.DefaultTraverse

package object iota {
  type ACopK[α] = CopK[_, α]
  type :<<:[F[_], G[α] <: ACopK[α]] = CopK.Inject[F, G]

  def computeListK[LL <: TListK, RR <: TListK, A](inp: CopK[LL, A])(implicit compute: TListK.Compute.Aux[LL, RR]): CopK[RR, A] = {
    val _ = compute
    inp.asInstanceOf[CopK[RR, A]]
  }

  def mkInject[F[_], LL <: TListK](i: Int): CopK.Inject[F, CopK[LL, ?]] =
    CopK.Inject.injectFromInjectL[F, LL](
      CopK.InjectL.makeInjectL[F, LL](
        new TListK.Pos[LL, F] { val index = i }))

  object functor {
    sealed trait FunctorMaterializer[LL <: TListK] {
      def materialize(offset: Int): Functor[CopK[LL, ?]]
    }
    implicit def base[F[_]: Functor]: FunctorMaterializer[F ::: TNilK] = new FunctorMaterializer[F ::: TNilK] {
      def materialize(offset: Int): Functor[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new Functor[CopK[F ::: TNilK, ?]] {
          def map[A, B](cfa: CopK[F ::: TNilK, A])(f: A => B): CopK[F ::: TNilK, B] = cfa match {
            case I(fa) => I(fa map f)
          }
        }
      }
    }
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_]: Functor, LL <: TListK](
        implicit LL: FunctorMaterializer[LL])
        : FunctorMaterializer[F ::: LL] = new FunctorMaterializer[F ::: LL] {
      def materialize(offset: Int): Functor[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new Functor[CopK[F ::: LL, ?]] {
          def map[A, B](cfa: CopK[F ::: LL, A])(f: A => B): CopK[F ::: LL, B] = cfa match {
            case I(fa) => I(fa map f)
            case other => LL.materialize(offset + 1).map(other.asInstanceOf[CopK[LL, A]])(f).asInstanceOf[CopK[F ::: LL, B]]
          }
        }
      }
    }
  }

  object eq {
    sealed trait DelayEqMaterializer[LL <: TListK] {
      def materialize(offset: Int): Delay[Eq, CopK[LL, ?]]
    }

    implicit def base[F[_]](implicit F: Delay[Eq, F]): DelayEqMaterializer[F ::: TNilK] = new DelayEqMaterializer[F ::: TNilK] {
      def materialize(offset: Int): Delay[Eq, CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new (Eq ~> λ[α => Eq[CopK[F ::: TNilK, α]]]) {
          def apply[A](eqa: Eq[A]): Eq[CopK[F ::: TNilK, A]] = new Eq[CopK[F ::: TNilK, A]] {
            def eqv(a: CopK[F ::: TNilK, A], b: CopK[F ::: TNilK, A]): Boolean = (a, b) match {
              case (I(left), I(right)) => F(eqa).eqv(left, right)
              case _ => false
            }
          }
        }
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_], LL <: TListK](
        implicit
        F: Delay[Eq, F],
        LL: DelayEqMaterializer[LL])
        : DelayEqMaterializer[F ::: LL] = new DelayEqMaterializer[F ::: LL] {
      def materialize(offset: Int): Delay[Eq, CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new (Eq ~> λ[α => Eq[CopK[F ::: LL, α]]]) {
          def apply[A](eqa: Eq[A]): Eq[CopK[F ::: LL, A]] = new Eq[CopK[F ::: LL, A]] {
            def eqv(a: CopK[F ::: LL, A], b: CopK[F ::: LL, A]) = (a, b) match {
              case ((I(left), I(right))) => F(eqa).eqv(left, right)
              case (left, right) => LL.materialize(offset + 1)(eqa).eqv(
                left.asInstanceOf[CopK[LL, A]],
                right.asInstanceOf[CopK[LL, A]])
            }
          }
        }
      }
    }
  }

  object traverse {
    sealed trait TraverseMaterializer[LL <: TListK] {
      def materialize(offset: Int): Traverse[CopK[LL, ?]]
    }

    implicit def base[F[_]: Traverse]: TraverseMaterializer[F ::: TNilK] = new TraverseMaterializer[F ::: TNilK] {
      def materialize(offset: Int): Traverse[CopK[F ::: TNilK, ?]] = {
        val I = mkInject[F, F ::: TNilK](offset)
        new DefaultTraverse[CopK[F ::: TNilK, ?]] {
          def traverse[G[_]: Applicative, A, B](cfa: CopK[F ::: TNilK, A])(f: A => G[B]): G[CopK[F ::: TNilK, B]] = cfa match {
            case I(fa) => fa.traverse(f).map(I(_))
          }
        }
      }
    }
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    implicit def induct[F[_]: Traverse, LL <: TListK](
        implicit LL: TraverseMaterializer[LL])
        : TraverseMaterializer[F ::: LL] = new TraverseMaterializer[F ::: LL] {
      def materialize(offset: Int): Traverse[CopK[F ::: LL, ?]] = {
        val I = mkInject[F, F ::: LL](offset)
        new DefaultTraverse[CopK[F ::: LL, ?]] {
          def traverse[G[_]: Applicative, A, B](cfa: CopK[F ::: LL, A])(f: A => G[B]): G[CopK[F ::: LL, B]] = cfa match {
            case I(fa) => fa.traverse(f).map(I(_))
            case other => LL.materialize(offset + 1).traverse(other.asInstanceOf[CopK[LL, A]])(f).asInstanceOf[G[CopK[F ::: LL, B]]]
          }
        }
      }
    }
  }

  import functor._
  import eq._
  import traverse._

  implicit def copkFunctor[LL <: TListK](implicit M: FunctorMaterializer[LL]): Functor[CopK[LL, ?]] =
    M.materialize(offset = 0)
  implicit def copkEq[LL <: TListK](implicit M: DelayEqMaterializer[LL]): Delay[Eq, CopK[LL, ?]] =
    M.materialize(offset = 0)
  implicit def copkTraverse[LL <: TListK](implicit M: TraverseMaterializer[LL]): Traverse[CopK[LL, ?]] =
    M.materialize(offset = 0)
}
