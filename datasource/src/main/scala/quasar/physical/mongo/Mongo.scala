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

import quasar.Disposable
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.physical.mongo.MongoResource.{Database, Collection}

import monocle.Prism
import cats.effect.{ConcurrentEffect, Async, IO}
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.applicative._
import cats.syntax.eq._

import fs2.Stream
import fs2.concurrent._
import org.bson.{Document => _, _}
import org.mongodb.scala._

class Mongo[F[_]: MonadResourceErr : ConcurrentEffect] private[Mongo](client: MongoClient) {
  import Mongo._

  val F: ConcurrentEffect[F] = ConcurrentEffect[F]

  def databases: Stream[F, Database] =
    observableAsStream(client.listDatabaseNames).map(Database(_))

  def databaseExists(database: Database): Stream[F, Boolean] =
    databases.exists(_ === database)

  def collections(database: Database): Stream[F, Collection] = for {
    dbExists <- databaseExists(database)
    res <- if (!dbExists) { Stream.empty } else {
      observableAsStream(client.getDatabase(database.name).listCollectionNames()).map(Collection(database, _))
    }
  } yield res

  def collectionExists(collection: Collection): Stream[F, Boolean] =
    collections(collection.database).exists(_ === collection)

  def findAll(collection: Collection): F[Stream[F, BsonValue]] = {
    def getCollection(mongo: MongoClient) =
      mongo.getDatabase(collection.database.name).getCollection(collection.name)

    collectionExists(collection).compile.last.map(_ getOrElse false)
      .flatMap(exists =>
        if (exists) {
          observableAsStream(getCollection(client).find[BsonValue]()).pure[F]
        } else {
          MonadResourceErr.raiseError(ResourceError.pathNotFound(collection.resourcePath))
        })
  }
}

object Mongo {

  sealed trait FromObservable[+A]

  object FromObservable {
    final case class Next[A](result: A) extends FromObservable[A]
    final case class Subscribe(subscription: Subscription) extends FromObservable[Nothing]
    final case class Error(throwable: Throwable) extends FromObservable[Nothing]

    def subscribeP[A]: Prism[FromObservable[A], Subscription] =
      Prism.partial[FromObservable[A], Subscription]{
        case Subscribe(s) => s
      }(Subscribe(_))

    def eraseSubscription[A](from: FromObservable[A]): Option[Either[Throwable, A]] = from match {
      case Next(result) => Some(Right(result))
      case Subscribe(_) => None
      case Error(e) => Some(Left(e))
    }
  }
  import FromObservable._


  def unsubscribe(sub: Option[Subscription]): Unit = sub.map { x =>
    if (!x.isUnsubscribed()) {
      x.unsubscribe()
    }
  } getOrElse (())

  def observableAsStream[F[_]: ConcurrentEffect, A](obs: Observable[A]): Stream[F, A] = {
    val F = ConcurrentEffect[F]
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    def handler(cb: Option[FromObservable[A]] => Unit): Unit = {
      obs.subscribe(new Observer[A] {
        private var subscription: Option[Subscription] = None

        override def onSubscribe(sub: Subscription): Unit = {
          subscription = Some(sub)
          sub.request(1L)
          cb(Some(Subscribe(sub)))
        }
        override def onNext(result: A): Unit = {
          cb(Some(Next(result)))
          subscription.map(_.request(1L)) getOrElse (())
        }
        override def onError(e: Throwable): Unit = cb(Some(Error(e)))
        override def onComplete(): Unit = cb(None)

      })
    }
    def enqueueObservable(
      obsQ: NoneTerminatedQueue[F, Either[Throwable, A]],
      subQ: NoneTerminatedQueue[F, Subscription]): Stream[F, Unit] =

    Stream.eval(F.delay(handler {inp =>
      def run(action: F[Unit]): Unit = F.runAsync(action)(_ => IO.unit).unsafeRunSync
      inp match {
        case Some(Subscribe(sub)) => run(subQ.enqueue1(Some(sub)))
        case Some(Error(e)) => run(obsQ.enqueue1(Some(Left(e))))
        case Some(Next(a)) => run(obsQ.enqueue1(Some(Right(a))))
        case None => run(obsQ.enqueue1(None))
      }}))

    for {
      obsQ <- Stream.eval(Queue.boundedNoneTerminated[F, Either[Throwable, A]](32))
      subQ <- Stream.eval(Queue.boundedNoneTerminated[F, Subscription](1))
      _ <- enqueueObservable(obsQ, subQ)
      sub <- subQ.dequeue.take(1)
      res <- obsQ.dequeue.rethrow.onFinalize(F.delay(sub.unsubscribe))
    } yield res
  }

  def singleObservableAsF[F[_]: Async, A](obs: SingleObservable[A]): F[A] =
    Async[F].async { cb: (Either[Throwable, A] => Unit) =>
      obs.subscribe(new Observer[A] {
        override def onNext(r: A): Unit = cb(Right(r))
        override def onError(e: Throwable): Unit = cb(Left(e))
        override def onComplete() = ()
      })
    }


  def apply[F[_]: ConcurrentEffect: MonadResourceErr](config: MongoConfig): F[Disposable[F, Mongo[F]]] = {

    val mkClient: F[MongoClient] =
      ConcurrentEffect[F].delay(MongoClient(config.connectionString))

    def runCommand(client: MongoClient): F[Unit] = singleObservableAsF[F, Unit](
      client.getDatabase("admin").runCommand[Document](Document("ping" -> 1)).map(_ => ())
    )

    def close(client: MongoClient): F[Unit] =
      ConcurrentEffect[F].delay(client.close())

    for {
      client <- mkClient
      _ <- runCommand(client)
    } yield Disposable(new Mongo[F](client), close(client))
  }
}
