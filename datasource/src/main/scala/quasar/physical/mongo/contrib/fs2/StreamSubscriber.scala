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

package quasar.physical.mongo.contrib.fs2

import slamdata.Predef._

import cats._
import cats.effect._
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.implicits._
import org.reactivestreams._

import fs2.concurrent.Queue
import fs2.{Chunk, Stream}

import java.lang.{Error, NullPointerException, Thread}

/**
  * Implementation of a `org.reactivestreams.Subscriber`.
  *
  * This is used to obtain a `fs2.Stream` from an upstream reactivestreams system.
  *
  * @see [[https://github.com/reactive-streams/reactive-streams-jvm#2-subscriber-code]]
  */
final class StreamSubscriber[F[_]: ConcurrentEffect, A](val sub: StreamSubscriber.FSM[F, A])
    extends Subscriber[A] {

  /** Called by an upstream reactivestreams system */
  def onSubscribe(s: Subscription): Unit = {
    nonNull(s)
    unsafeRunAsync(sub.onSubscribe(s))
  }

  /** Called by an upstream reactivestreams system */
  def onNext(a: A): Unit = {
    nonNull(a)
    unsafeRunAsync(sub.onNext(a))
  }

  /** Called by an upstream reactivestreams system */
  def onComplete(): Unit = unsafeRunAsync(sub.onComplete)

  /** Called by an upstream reactivestreams system */
  def onError(t: Throwable): Unit = {
    nonNull(t)
    unsafeRunAsync(sub.onError(t))
  }

  /** Obtain a fs2.Stream */
  @deprecated(
    "subscribing to a publisher prior to pulling the stream is unsafe if interrupted",
    "2.2.3"
  )
  def stream: Stream[F, A] = stream(().pure[F])

  def stream(subscribe: F[Unit]): Stream[F, A] = sub.stream(subscribe)

  private def nonNull[B](b: B): Unit = if (b == null) throw new NullPointerException()

  private def unsafeRunAsync[G[_]: ConcurrentEffect, B](gb: G[B]): Unit = {
    def reportFailure(e: Throwable) =
      Thread.getDefaultUncaughtExceptionHandler match {
        case null => e.printStackTrace()
        case h    => h.uncaughtException(Thread.currentThread(), e)
      }

    gb.runAsync {
      case Left(e)  => IO(reportFailure(e))
      case Right(_) => IO.unit
    }.unsafeRunSync
  }
}

object StreamSubscriber {
  def apply[F[_]: ConcurrentEffect, A]: F[StreamSubscriber[F, A]] =
    apply[F, A](1)

  def apply[F[_]: ConcurrentEffect, A](maxDemand: Int): F[StreamSubscriber[F, A]] =
    Queue
      .bounded[F, Either[Throwable, Option[A]]](maxDemand + 2)
      .flatMap(fsm[F, A](_, maxDemand).map(new StreamSubscriber(_)))

  def fromPublisher[F[_]: ConcurrentEffect, A](p: Publisher[A], batchSize: Int = 1): Stream[F, A] =
    Stream
      .eval(StreamSubscriber[F, A](batchSize))
      .flatMap(s => s.sub.stream(Sync[F].delay(p.subscribe(s))))

  /** A finite state machine describing the subscriber */
  trait FSM[F[_], A] {

    /** receives a subscription from upstream */
    def onSubscribe(s: Subscription): F[Unit]

    /** receives next record from upstream */
    def onNext(a: A): F[Unit]

    /** receives error from upstream */
    def onError(t: Throwable): F[Unit]

    /** called when upstream has finished sending records */
    def onComplete: F[Unit]

    /** called when downstream has finished consuming records */
    def onFinalize: F[Unit]

    /** producer for downstream */
    def dequeueChunk1: F[Chunk[Either[Throwable, Option[A]]]]

    /** downstream stream */
    def stream(subscribe: F[Unit])(implicit ev: ApplicativeError[F, Throwable]): Stream[F, A] =
      Stream.bracket(subscribe)(_ => onFinalize) >> Stream
        .evalUnChunk(dequeueChunk1)
        .repeat
        .rethrow
        .unNoneTerminate
  }

  def fsm[F[_], A](q: Queue[F, Either[Throwable, Option[A]]], maxDemand: Int)(
    implicit F: Concurrent[F]): F[FSM[F, A]] = {

    sealed trait Input
    case class OnSubscribe(s: Subscription) extends Input
    case class OnNext(a: A) extends Input
    case class OnError(e: Throwable) extends Input
    case object OnComplete extends Input
    case object OnFinalize extends Input
    case object OnDequeue extends Input

    sealed trait State
    case object Uninitialized extends State
    case class Idle(sub: Subscription) extends State
    case class Receiving(sub: Subscription) extends State
    case object RequestBeforeSubscription extends State
    case object UpstreamCompletion extends State
    case object DownstreamCancellation extends State
    case class UpstreamError(err: Throwable) extends State

    def step(in: Input): State => (State, F[Unit]) =
      in match {
        case OnSubscribe(s) => {
          case RequestBeforeSubscription =>
            Receiving(s) -> F.delay(s.request(maxDemand.toLong))
          case Uninitialized =>
            Idle(s) -> F.unit
          case Idle(prev) => // we got a second subscription. cancel it
            Idle(prev) -> F.delay(s.cancel)
          case Receiving(prev) => // we got a second subscription. cancel it
            Receiving(prev) -> F.delay(s.cancel)
          case DownstreamCancellation =>
            DownstreamCancellation -> F.delay(s.cancel)
          case o =>
            val err = new Error(s"received subscription in invalid state [$o]")
            o -> (F.delay(s.cancel) >> F.raiseError(err))
        }
        case OnNext(a) => {
          case Receiving(s) =>
            Receiving(s) -> q.enqueue1(a.some.asRight)
          case Idle(s) =>
            Receiving(s) -> q.enqueue1(a.some.asRight)
          case DownstreamCancellation =>
            DownstreamCancellation -> F.unit
          case o =>
            o -> F.raiseError(new Error(s"received record [$a] in invalid state [$o]"))
        }
        case OnComplete => {
          case Receiving(_) | Idle(_) =>
            UpstreamCompletion -> q.enqueue1(None.asRight)
          case _ =>
            UpstreamCompletion -> F.unit
        }
        case OnError(e) => {
          case Receiving(_) | Idle(_) =>
            UpstreamError(e) -> (q.enqueue1(e.asLeft) >> q.enqueue1(None.asRight))
          case _ =>
           UpstreamError(e) -> F.unit
        }
        case OnFinalize => {
          case Idle(sub) =>
            DownstreamCancellation -> (F.delay(sub.cancel) >> q.enqueue1(None.asRight))
          case Receiving(sub) =>
            DownstreamCancellation -> (F.delay(sub.cancel) >> q.enqueue1(None.asRight))
          case Uninitialized | RequestBeforeSubscription =>
            DownstreamCancellation -> F.unit
          case o =>
            o -> F.unit
        }
        case OnDequeue => {
          case Uninitialized =>
            RequestBeforeSubscription -> F.unit
          case Idle(sub) =>
            Receiving(sub) -> F.delay(sub.request(maxDemand.toLong)) // request on first dequeue
          case st =>
            st -> F.unit
        }
      }

    Ref.of[F, State](Uninitialized) map { ref =>
      new FSM[F, A] {
        def nextState(in: Input): F[Unit] =
          ref.modify(step(in)).flatten
        def onSubscribe(s: Subscription): F[Unit] =
          nextState(OnSubscribe(s))
        def onNext(a: A): F[Unit] =
          nextState(OnNext(a))
        def onError(t: Throwable): F[Unit] =
          nextState(OnError(t))
        def onComplete: F[Unit] =
          nextState(OnComplete)
        def onFinalize: F[Unit] =
          nextState(OnFinalize)
        def dequeueChunk1: F[Chunk[Either[Throwable, Option[A]]]] =
          for {
            _ <- ref.modify(step(OnDequeue)).flatten
            chunk <- q.dequeueChunk1(maxDemand)
            _ <- ref.get flatMap {
              case Receiving(s) => F.delay(s.request(chunk.size.toLong))
              case _ => F.unit
            }
          } yield chunk
      }
    }
  }
}
