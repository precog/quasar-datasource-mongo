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

import cats.effect.{Async, ConcurrentEffect, IO}
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._

import fs2.concurrent.Queue
import fs2.Stream

import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.physical.mongo.MongoResource.{Database, Collection}
import quasar.Disposable

import com.mongodb.ConnectionString

import org.bson.{Document => _, _}
import org.mongodb.scala._
import org.mongodb.scala.connection.NettyStreamFactoryFactory

class Mongo[F[_]: MonadResourceErr : ConcurrentEffect] private[Mongo](client: MongoClient, queueSize: Int) {
  val F: ConcurrentEffect[F] = ConcurrentEffect[F]

  private def observableAsStream[A](obs: Observable[A]): Stream[F, A] = {
    val back = for {
      as <- Queue.boundedNoneTerminated[F, Either[Throwable, A]](queueSize)

      sub <- F.async[Subscription] { k =>
        obs.subscribe(new Observer[A] {

          override def onSubscribe(sub: Subscription): Unit =
            k(Right(sub))

          override def onNext(result: A): Unit =
            toIO(as.enqueue1(Some(Right(result)))).unsafeRunSync()

          override def onError(e: Throwable): Unit =
            toIO(as.enqueue1(Some(Left(e)))).unsafeRunSync()

          override def onComplete(): Unit =
            toIO(as.enqueue1(None)).unsafeRunSync()

          private[this] def toIO(fu: F[Unit]): IO[Unit] =
            IO.asyncF(k => F.runAsync(fu)(e => IO(k(e))).toIO)
        })
      }

      // request queueSize, then take from the queue until we get that much, or until we barf; rechunking is dynamic depending on downstream consumption rate
      step = Stream.eval_(F.delay(sub.request(queueSize.toLong))) ++ as.dequeue.rethrow.take(queueSize.toLong)
    } yield step.repeat.onComplete(Stream.eval_(F.delay(if (!sub.isUnsubscribed()) sub.unsubscribe())))

    Stream.eval(back).flatten
  }

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
  def singleObservableAsF[F[_]: Async, A](obs: SingleObservable[A]): F[A] =
    Async[F].async { cb: (Either[Throwable, A] => Unit) =>
      obs.subscribe(new Observer[A] {
        override def onNext(r: A): Unit = cb(Right(r))
        override def onError(e: Throwable): Unit = cb(Left(e))
        override def onComplete() = ()
      })
    }

  def apply[F[_]: ConcurrentEffect: MonadResourceErr](config: MongoConfig, queueSize: Int): F[Disposable[F, Mongo[F]]] = {
    val F = ConcurrentEffect[F]

    val mkClient: F[MongoClient] = for {
      connString <- F.delay(new ConnectionString(config.connectionString))
      connStringSettings <- F.delay(MongoClientSettings.builder().applyConnectionString(connString).build())
      settings <- F.delay(if (connStringSettings.getSslSettings.isEnabled) {
        MongoClientSettings.builder(connStringSettings).streamFactoryFactory(NettyStreamFactoryFactory()).build()
      } else connStringSettings)
      client <- F.delay(MongoClient(settings))
    } yield client

    def runCommand(client: MongoClient): F[Unit] =
      F.suspend(singleObservableAsF[F, Unit](
        client.getDatabase("admin").runCommand[Document](Document("ping" -> 1)).map(_ => ())
      ))

    def close(client: MongoClient): F[Unit] =
      F.delay(client.close())

    for {
      client <- mkClient
      _ <- runCommand(client)
    } yield Disposable(new Mongo[F](client, queueSize), close(client))
  }
}
