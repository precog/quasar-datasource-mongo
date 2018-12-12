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

import cats.effect.{ConcurrentEffect, Async, IO}
import cats.effect.concurrent.MVar
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._

import fs2.concurrent.{Queue, NoneTerminatedQueue}
import fs2.Stream

import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.physical.mongo.MongoResource.{Database, Collection}
import quasar.Disposable

import com.mongodb.ConnectionString

import org.bson.{Document => _, _}
import org.mongodb.scala._
import org.mongodb.scala.connection.NettyStreamFactoryFactory

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
          F.delay(observableAsStream(getCollection(client).find[BsonValue]()))
        } else {
          MonadResourceErr.raiseError(ResourceError.pathNotFound(collection.resourcePath))
        })
  }
}

object Mongo {

  def observableAsStream[F[_]: ConcurrentEffect, A](obs: Observable[A]): Stream[F, A] = {
    val queueSize: Int = 2048

    val F = ConcurrentEffect[F]

    def run(action: F[Unit]): Unit = F.runAsync(action)(_ => IO.unit).unsafeRunSync

    def handler(subVar: MVar[F, Subscription], cb: Option[Either[Throwable, A]] => Unit): Unit = {
      obs.subscribe(new Observer[A] {
        override def onSubscribe(sub: Subscription): Unit = {
          run(subVar.put(sub))
          sub.request(queueSize.toLong)
        }
        override def onNext(result: A): Unit =
          cb(Some(Right(result)))

        override def onError(e: Throwable): Unit = cb(Some(Left(e)))

        override def onComplete(): Unit = cb(None)
      })
    }

    def unsubscribe(s: Subscription): F[Unit] = F.delay {
      if (!s.isUnsubscribed()) s.unsubscribe()
    }

    def enqueueObservable(
        obsQ: NoneTerminatedQueue[F, Either[Throwable, A]],
        subVar: MVar[F, Subscription])
        : Stream[F, Unit] =
      Stream.eval(F.delay(handler(subVar, { x => run(obsQ.enqueue1(x)) })))

    (for {
      obsQ <- Stream.eval(Queue.boundedNoneTerminated[F, Either[Throwable, A]](queueSize))
      subVar <- Stream.eval(MVar[F].empty[Subscription])
      _ <- enqueueObservable(obsQ, subVar)
      res <- obsQ.dequeue.chunkN(queueSize, true).flatMap( c =>
        Stream.evalUnChunk(subVar.read.map(_.request(c.size.toLong)).as(c))
      ).rethrow.onFinalize(subVar.take.flatMap(unsubscribe))
    } yield res)
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

    def delay[A](a: A): F[A] = ConcurrentEffect[F].delay(a)

    val mkClient: F[MongoClient] = for {
      connString <- delay(new ConnectionString(config.connectionString))
      connStringSettings <- delay(MongoClientSettings.builder().applyConnectionString(connString).build())
      settings <- delay(if (connStringSettings.getSslSettings.isEnabled) {
        MongoClientSettings.builder(connStringSettings).streamFactoryFactory(NettyStreamFactoryFactory()).build()
      } else connStringSettings)
      client <- delay(MongoClient(settings))
    } yield client

    def runCommand(client: MongoClient): F[Unit] =
      ConcurrentEffect[F].suspend(singleObservableAsF[F, Unit](
        client.getDatabase("admin").runCommand[Document](Document("ping" -> 1)).map(_ => ())
      ))

    def close(client: MongoClient): F[Unit] =
      ConcurrentEffect[F].delay(client.close())

    for {
      client <- mkClient
      _ <- runCommand(client)
    } yield Disposable(new Mongo[F](client), close(client))
  }
}
