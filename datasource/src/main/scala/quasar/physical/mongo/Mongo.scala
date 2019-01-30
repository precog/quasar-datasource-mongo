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

import cats.effect.{Async, ConcurrentEffect, IO, Sync}
import cats.effect.concurrent.MVar
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._

import fs2.concurrent.{NoneTerminatedQueue, Queue}
import fs2.Stream

import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.physical.mongo.MongoResource.{Collection, Database}
import quasar.Disposable
import quasar.api.resource.ResourcePath

import org.bson.{Document => _, _}
import com.mongodb.ConnectionString
import org.mongodb.scala._
import org.mongodb.scala.connection.NettyStreamFactoryFactory

class Mongo[F[_]: MonadResourceErr : ConcurrentEffect] private[mongo](
    client: MongoClient,
    maxMemory: Int,
    val accessedResource: Option[MongoResource]) {
  import Mongo._

  val F: ConcurrentEffect[F] = ConcurrentEffect[F]

  private def observableAsStream[A](obs: Observable[A], queueSize: Long): Stream[F, A] = {
    def run(action: F[Unit]): Unit = F.runAsync(action)(_ => IO.unit).unsafeRunSync

    def handler(subVar: MVar[F, Subscription], cb: Option[Either[Throwable, A]] => Unit): Unit = {
      obs.subscribe(new Observer[A] {
        override def onSubscribe(sub: Subscription): Unit = {
          run(subVar.put(sub))
          sub.request(queueSize)
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
      obsQ <- Stream.eval(Queue.boundedNoneTerminated[F, Either[Throwable, A]](queueSize.toInt))
      subVar <- Stream.eval(MVar[F].empty[Subscription])
      _ <- enqueueObservable(obsQ, subVar)
      res <- obsQ.dequeue.chunks.flatMap( c =>
        Stream.evalUnChunk(subVar.read.map(_.request(c.size.toLong)).as(c))
      ).rethrow.onFinalize(subVar.take.flatMap(unsubscribe))
    } yield res)
  }

  private def mkMsg(ex: MongoCommandException) = s"Mongo command error: ${ex.getErrorCodeName}"

  object MongoConnectionFailed {
    def unapply(t: Throwable): Option[(Throwable, String)] = t match {
      case ex: MongoTimeoutException => Some((ex, "Timed out connecting to server"))
      // see for a list of codes: https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err
      case ex: MongoCommandException if List(6, 7, 24, 89, 230, 231, 240).contains(ex.getErrorCode) =>
        Some((ex, mkMsg(ex)))
      case _ => None
    }
  }

  object MongoAccessDenied {
    def unapply(t: Throwable): Option[(Throwable, String)] = t match {
      case ex: MongoSecurityException => Some((ex, "Client authentication error"))
      // see for a list of codes: https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err
      case ex: MongoCommandException if List(11, 13, 18, 31, 33).contains(ex.getErrorCode) =>
        Some((ex, mkMsg(ex)))
      case _ => None
    }
  }

  def errorHandler[A](path: ResourcePath): Throwable => Stream[F, A] = {
    case MongoConnectionFailed((ex, detail)) =>
      Stream.eval(MonadResourceErr.raiseError(ResourceError.connectionFailed(path, Some(detail), Some(ex))))
    case MongoAccessDenied((ex, detail)) =>
      Stream.eval(MonadResourceErr.raiseError(ResourceError.accessDenied(path, Some(detail), Some(ex))))
    case t => Stream.raiseError(t)
  }

  def databases: Stream[F, Database] = {

    val fallbackDb = accessedResource.map(MongoResource.getDatabase)

    def emitFallbackOr(ifEmpty: => Stream[F, Database]) =
      fallbackDb.fold[Stream[F, Database]](ifEmpty)(Stream.emit)

    val recoverAccessDenied: Throwable => Stream[F, Database] = {
      case MongoAccessDenied((ex, _)) => emitFallbackOr(Stream.raiseError(ex))
      case t: Throwable => Stream.raiseError(t)
    }

    val dbs = observableAsStream(client.listDatabaseNames, DefaultQueueSize)
      .map(Database(_))
      .handleErrorWith(recoverAccessDenied)
      .handleErrorWith(errorHandler(ResourcePath.root()))

    Stream.force(dbs.compile.toList.map { (list: List[Database]) =>
      if (list.isEmpty) emitFallbackOr(Stream.empty) else Stream.emits(list)
    })
  }

  def databaseExists(database: Database): Stream[F, Boolean] =
    databases.exists(_ === database)

  def collections(database: Database): Stream[F, Collection] = {

    def emitFallbackOr(s: => Stream[F, Collection]) = accessedResource match {
      case Some(coll @ Collection(_, _)) if database === coll.database => Stream.emit(coll)
      case Some(db @ Database(_)) if db === database => Stream.empty
      case _ => s
    }

    val recoverAccessDenied: Throwable => Stream[F, Collection] = {
      case MongoAccessDenied((ex, _)) => emitFallbackOr(Stream.raiseError(ex))
      case t: Throwable => Stream.raiseError(t)
    }

    val cols = for {
      dbExists <- databaseExists(database)
      res <- if (!dbExists) Stream.empty else
        observableAsStream(client.getDatabase(database.name).listCollectionNames(), DefaultQueueSize).map(Collection(database, _))
          .handleErrorWith(recoverAccessDenied)
          .handleErrorWith(errorHandler(database.resourcePath))
    } yield res

    Stream.force(cols.compile.toList.map { (list: List[Collection]) =>
      if (list.isEmpty) emitFallbackOr(Stream.empty) else Stream.emits(list)
    })
  }


  def collectionExists(collection: Collection): Stream[F, Boolean] =
    collections(collection.database).exists(_ === collection)

  def findAll(collection: Collection): F[Stream[F, BsonValue]] = {
    collectionExists(collection).compile.last.map(_ getOrElse false)
      .flatMap(exists =>
        if (exists) {
          getQueueSize(collection).map { (qs: Long) =>
            observableAsStream(getCollection(collection).find[BsonValue](), qs)
          }
        } else {
          MonadResourceErr.raiseError(ResourceError.pathNotFound(collection.resourcePath))
        })
  }

  private val maximumBatchBytes: Long = if (maxMemory > MaxBsonBatch) MaxBsonBatch else maxMemory.toLong

  def getQueueSize(collection: Collection): F[Long] = {
    val getStats: F[Option[Document]] = F.attempt(F.suspend(singleObservableAsF[F, Document](
      client.getDatabase(collection.database.name).runCommand[Document](Document(
        "collStats" -> collection.name,
        "scale" -> 1
      ))
    ))).map(_.toOption)

    getStats map { doc =>
      val optSize: Option[Long] = doc.flatMap (_.get("avgObjSize")) flatMap (_ match {
        case x: BsonInt32 => Some(x.getValue().toLong)
        case x: BsonInt64 => Some(x.getValue())
        case _ => None
      })
      optSize.fold(DefaultQueueSize)(x => scala.math.max(1L, maximumBatchBytes / x))
    }
  }

  private def getCollection(collection: Collection): MongoCollection[Document] =
    client.getDatabase(collection.database.name).getCollection(collection.name)

}

object Mongo {
  val DefaultBsonBatch: Int = 1024 * 128
  val MaxBsonBatch: Long = 128L * 1024L * 1024L
  val DefaultQueueSize: Long = 256L

  def singleObservableAsF[F[_]: Async, A](obs: SingleObservable[A]): F[A] =
    Async[F].async { cb: (Either[Throwable, A] => Unit) =>
      obs.subscribe(new Observer[A] {
        override def onNext(r: A): Unit = cb(Right(r))
        override def onError(e: Throwable): Unit = cb(Left(e))
        override def onComplete() = ()
      })
    }

  def mkClient[F[_]](config: MongoConfig)(implicit F: Sync[F]): F[MongoClient] =
    for {
      connString <- F.delay(new ConnectionString(config.connectionString))
      connStringSettings <- F.delay(MongoClientSettings.builder().applyConnectionString(connString).build())
      settings <- F.delay(
        if (connStringSettings.getSslSettings.isEnabled)
          MongoClientSettings.builder(connStringSettings).streamFactoryFactory(NettyStreamFactoryFactory()).build()
        else connStringSettings)
      client <- F.delay(MongoClient(settings))
    } yield client

  def close[F[_]](client: MongoClient)(implicit F: Sync[F]): F[Unit] =
    F.delay(client.close())

  def apply[F[_]: ConcurrentEffect: MonadResourceErr](config: MongoConfig): F[Disposable[F, Mongo[F]]] = {
    val F = ConcurrentEffect[F]

    def runCommand(client: MongoClient): F[Unit] =
      F.suspend(singleObservableAsF[F, Unit](
        client.getDatabase("admin").runCommand[Document](Document("ping" -> 1)).map(_ => ())
      ))

    for {
      client <- mkClient(config)
      _ <- runCommand(client)
    } yield Disposable(
      new Mongo[F](
        client,
        config.resultBatchSizeBytes.getOrElse(DefaultBsonBatch),
        config.accessedResource),
      close(client))
  }
}
