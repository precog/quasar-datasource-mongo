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

import cats.effect._
import cats.effect.concurrent.MVar
import cats.effect.syntax.bracket._
import cats.implicits._

import fs2.concurrent.{NoneTerminatedQueue, Queue}
import fs2.Stream

import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, Offset, ResourceError}
import quasar.physical.mongo.MongoResource.{Collection, Database}
import quasar.{IdStatus, ScalarStages}

import org.bson.{Document => _, _}
import com.mongodb.{Block, ConnectionString}
import com.mongodb.connection.{ClusterSettings, SslSettings}
import org.mongodb.scala._

import shims.equalToCats

final class Mongo[F[_]: MonadResourceErr: ConcurrentEffect: ContextShift] private[mongo](
    client: MongoClient,
    batchSize: Long,
    pushdownLevel: PushdownLevel,
    interpret: Interpreter.StageInterpret,
    offsetInterpret: Interpreter.OffsetInterpret,
    accessedResource: Option[MongoResource]) {
  import Mongo._

  val F: ConcurrentEffect[F] = ConcurrentEffect[F]

  private def observableAsStream[A](obs: Observable[A], queueSize: Long): Stream[F, A] = {
    def run(action: F[Unit]): Unit = F.runAsync(action)(_ => IO.unit).unsafeRunSync

    def handler(subVar: MVar[F, Subscription], cb: Option[Either[Throwable, A]] => Unit): Unit = {
      obs.subscribe(new Observer[A] {
        override def onSubscribe(sub: Subscription): Unit = run {
          for {
            _ <- subVar.put(sub)
            _ <- request(sub, queueSize)
          } yield ()
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

    // This is necessary because `.request(size)` depends on internal driver state
    // that might be changed by threads that we can't control, and `.request` can throw an error.
    // We can't rely on `MVar[F, Option[Subscription]]` because actual change of internal state happens
    // in java driver depth and it might happen even during flatMap.
    def request(s: Subscription, size: Long): F[Unit] =
      F.attempt(F.delay(s.request(size))).void

    (for {
      obsQ <- Stream.eval(Queue.boundedNoneTerminated[F, Either[Throwable, A]](queueSize.toInt))
      subVar <- Stream.eval(MVar[F].empty[Subscription])
      _ <- enqueueObservable(obsQ, subVar)
      res <- obsQ.dequeue.chunks.flatMap( c =>
        Stream.evalUnChunk(subVar.read.flatMap(request(_, c.size.toLong)).as(c))
      ).rethrow.onFinalize(subVar.take.flatMap(unsubscribe))
    } yield res)
  }

  def errorHandler[A](path: ResourcePath): Throwable => Stream[F, A] = {
    case MongoConnectionFailed((ex, detail)) =>
      Stream.eval(MonadResourceErr.raiseError(ResourceError.connectionFailed(path, Some(detail), Some(ex))))
    case MongoAccessDenied((ex, detail)) =>
      Stream.eval(MonadResourceErr.raiseError(ResourceError.accessDenied(path, Some(detail), Some(ex))))
    case t => Stream.raiseError[F](t)
  }

  def databases: Stream[F, Database] = {
    val fallbackDb = accessedResource.map(MongoResource.getDatabase)

    def emitFallbackOr(ifEmpty: => Stream[F, Database]) =
      fallbackDb.fold[Stream[F, Database]](ifEmpty)(Stream.emit)

    val recoverAccessDenied: Throwable => Stream[F, Database] = {
      case MongoAccessDenied((ex, _)) => emitFallbackOr(Stream.raiseError[F](ex))
      case t: Throwable => Stream.raiseError[F](t)
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
      case MongoAccessDenied((ex, _)) => emitFallbackOr(Stream.raiseError[F](ex))
      case t: Throwable => Stream.raiseError[F](t)
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

  private def withCollectionExists[A](collection: Collection, obs: Observable[A]): F[Stream[F, A]] =
    collectionExists(collection).compile.last map(_ getOrElse false) flatMap { exists =>
      if (exists) F.point(observableAsStream(obs, batchSize))
      else MonadResourceErr.raiseError(ResourceError.pathNotFound(collection.resourcePath))
    }

  def findAll(collection: Collection): F[Stream[F, BsonValue]] =
    withCollectionExists(collection, getCollection(collection).find[BsonValue]())

  def aggregate(collection: Collection, aggs: List[BsonDocument]): F[Stream[F, BsonValue]] =
    withCollectionExists(
      collection,
      getCollection(collection).aggregate[BsonValue](aggs)
        .allowDiskUse(true))

  def evaluateImpl(collection: Collection, aggs: List[BsonDocument])
      : F[Either[Throwable, Stream[F, BsonValue]]] = {

    val aggregated =
      aggregate(collection, aggs)

    val hd: F[Either[Throwable, Option[BsonValue]]] =
      F.attempt(aggregated flatMap (_.head.compile.last))

    hd.flatMap(_.as(aggregated).sequence)
  }

  def evaluate(collection: Collection, stages: ScalarStages, offset: Option[Offset])
      : F[(ScalarStages, Stream[F, BsonValue])] = {
    (stages, pushdownLevel, offset) match {
      case (ScalarStages.Id, _, Some(offset)) =>
        evaluateNoPushdownWithOffset(collection, stages, offset)

      case (_, PushdownLevel.Disabled, Some(offset)) =>
        evaluateNoPushdownWithOffset(collection, stages, offset)

      case (_, _, offset) =>
        val (interpretation, mapper) = interpret(stages, offset)

        val fallback: F[(ScalarStages, Stream[F, BsonValue])] =
          findAll(collection).tupleLeft(stages)

        if (interpretation.docs.isEmpty)
          fallback
        else
          evaluateImpl(collection, interpretation.docs) flatMap {
            case Right(stream) =>
              val newStream = stream map mapper.bson
              val newStages = ScalarStages(IdStatus.ExcludeId, interpretation.stages)

              (newStages, newStream).pure[F]

            case _ => fallback
          }
    }
  }

  private [mongo] def getCollection(collection: Collection): MongoCollection[Document] =
    client.getDatabase(collection.database.name).getCollection(collection.name)

  private def evaluateNoPushdownWithOffset(collection: Collection, stages: ScalarStages, offset: Offset)
      : F[(ScalarStages, Stream[F, BsonValue])] =
    offsetInterpret(offset) match {
      case Some(pipelineDocs) =>
        evaluateImpl(collection, pipelineDocs) flatMap {
          case Right(results) => (stages, results).pure[F]
          case Left(_) => MonadResourceErr[F].raiseError(ResourceError.seekFailed(collection.resourcePath))
        }
      case None =>
        MonadResourceErr[F].raiseError(ResourceError.seekFailed(collection.resourcePath))
    }
}

object Mongo {
  val DefaultQueueSize: Long = 256L

  private def mkMsg(ex: MongoCommandException) = s"Mongo command error: ${ex.getErrorCodeName}"

  object MongoConnectionFailed {
    def unapply(t: Throwable): Option[(Exception, String)] = t match {
      case ex: MongoTimeoutException => Some((ex, "Timed out connecting to server"))
      case ex: MongoSocketException => Some((ex, "Error connecting to server"))
      // see for a list of codes: https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err
      case ex: MongoCommandException if List(6, 7, 24, 89, 230, 231, 240).contains(ex.getErrorCode) =>
        Some((ex, mkMsg(ex)))
      case _ => None
    }
  }

  object MongoAccessDenied {
    def unapply(t: Throwable): Option[(Exception, String)] = t match {
      case ex: MongoSecurityException => Some((ex, "Client authentication error"))
      // see for a list of codes: https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err
      case ex: MongoCommandException if List(11, 13, 18, 31, 33).contains(ex.getErrorCode) =>
        Some((ex, mkMsg(ex)))
      case _ => None
    }
  }

  def singleObservableAsF[F[_]: Async: ContextShift, A](obs: SingleObservable[A]): F[A] =
    Async[F].async { cb: (Either[Throwable, A] => Unit) =>
      obs.subscribe(new Observer[A] {
        override def onNext(r: A): Unit = cb(Right(r))

        override def onError(e: Throwable): Unit = cb(Left(e))

        override def onComplete() = ()
      })
    } guarantee ContextShift[F].shift

  def mkClient[F[_]: ContextShift](
      config: MongoConfig,
      blocker: Blocker)(
      implicit F: Sync[F])
      : Resource[F, MongoClient] =
    Settings[F](config, blocker) flatMap { clusterSettings =>
      Resource(for {
        conn <- F.delay { new ConnectionString(config.connectionString) }

        rawSettings <- F.delay {
          val updateCluster: Block[ClusterSettings.Builder] = new Block[ClusterSettings.Builder] {
            def apply(t: ClusterSettings.Builder): Unit = {
              val _ = t.applySettings(clusterSettings); ()
            }
          }
          MongoClientSettings.builder.applyConnectionString(conn).applyToClusterSettings(updateCluster).build
        }

        rawSslSettings <- F.delay(SslSettings.builder.applyConnectionString(conn).build)
        sslConfig = if (rawSslSettings.isEnabled) {
          config.sslConfig.orElse(Some(SSLConfig(None, None, true, None)))
        } else config.sslConfig
        sslSettings <- sslConfig match {
          case None => F.delay(rawSslSettings)
          case Some(cfg) =>
            SSL.context[F](cfg).value.flatMap {
              case None => F.delay(rawSslSettings)
              case Some(context) => F.delay {
                SslSettings
                  .builder
                  .applySettings(rawSslSettings)
                  .context(context)
                  .invalidHostNameAllowed(cfg.allowInvalidHostnames)
                  .build
              }
          }
        }

        updateSsl: Block[SslSettings.Builder] = new Block[SslSettings.Builder] {
          def apply(t: SslSettings.Builder): Unit = {
            val _ = t.applySettings(sslSettings); ()
          }
        }

        settings <- F.delay {
          if (sslSettings.isEnabled)
            MongoClientSettings
              .builder(rawSettings)
              .applyToSslSettings(updateSsl)
              .build
          else rawSettings
        }
        client <- F.delay(MongoClient(settings))
      } yield (client, close(client)))
    }

  def close[F[_]](client: MongoClient)(implicit F: Sync[F]): F[Unit] =
    F.delay(client.close())

  def apply[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr](
      config: MongoConfig,
      blocker: Blocker)
      : Resource[F, Mongo[F]] = {
    val F = ConcurrentEffect[F]

    def buildInfo(client: MongoClient): F[Document] =
      F.suspend(singleObservableAsF[F, Document](client.getDatabase("admin")
        .runCommand[Document](Document("buildInfo" -> 1))))

    def getVersionString(doc: Document): Option[String] =
      doc.get("version") flatMap {
        case x: BsonString => Some(x.getValue())
        case _ => None
      }

    def mkVersion(parts: Array[String]): Option[Version] = for {
      majorString <- parts.lift(0)
      minorString <- parts.lift(1)
      patchString <- parts.lift(2)
      major <- scala.Either.catchNonFatal(majorString.toInt).toOption
      minor <- scala.Either.catchNonFatal(minorString.toInt).toOption
      patch <- scala.Either.catchNonFatal(patchString.toInt).toOption
    } yield Version(major, minor, patch)


    // I don't think that there would be a lot of usability in having Option[Version] instead of Version
    // because we're going to use minimal values, it should be valid to simply return Version(0, 0, 0) in
    // case of error of decoding or something, although this checks that db is reachable
    def getVersion(client: MongoClient): F[Version] =
      buildInfo(client) map { x =>
        getVersionString(x) map (_.split("\\.")) flatMap mkVersion getOrElse Version.zero
      }

    mkClient(config, blocker) evalMap { client =>
      for {
        version <- getVersion(client)
        uuid <- Sync[F].delay(java.util.UUID.randomUUID.toString)
        stageInterpreter = Interpreter.stages(version, config.pushdownLevel, uuid)
        offsetInterpreter = Interpreter.offset(version, uuid)
        batchSize = scala.math.max(1L, config.batchSize.toLong)
      } yield new Mongo[F](
        client,
        batchSize,
        config.pushdownLevel,
        stageInterpreter,
        offsetInterpreter,
        config.accessedResource)
    }
  }
}
