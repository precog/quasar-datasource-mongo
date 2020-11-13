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
import scala.util.Either

import cats.effect._
import cats.implicits._

import fs2.Stream
import fs2.interop.reactivestreams._

import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.physical.mongo.MongoResource.{Collection, Database}
import quasar.{IdStatus, ScalarStages}

import org.bson.{Document => _, _}

import org.mongodb.scala._
import org.mongodb.scala.connection.{ClusterSettings, SslSettings}

import com.mongodb.Block

import java.lang.{ClassCastException, NumberFormatException}

import shims.equalToCats

final class Mongo[F[_]: MonadResourceErr: ConcurrentEffect: ContextShift] private[mongo](
    client: MongoClient,
    batchSize: Int,
    pushdownLevel: PushdownLevel,
    interpret: Interpreter.StageInterpret,
    offsetInterpret: Interpreter.OffsetInterpret,
    accessedResource: Option[MongoResource]) {
  import Mongo._

  val F: ConcurrentEffect[F] = ConcurrentEffect[F]

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

    val dbs =
      client.listDatabaseNames
        .toStream
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
        client.getDatabase(database.name).listCollectionNames.toStream
          .map(Collection(database, _))
          .handleErrorWith(recoverAccessDenied)
          .handleErrorWith(errorHandler(database.resourcePath))
    } yield res

    Stream.force(cols.compile.toList.map { (list: List[Collection]) =>
      if (list.isEmpty) emitFallbackOr(Stream.empty) else Stream.emits(list)
    })
  }

  def collectionExists(collection: Collection): Stream[F, Boolean] =
    collections(collection.database).exists(_ === collection)

  private def withCollectionExists[A](collection: Collection, str: Stream[F, A]): F[Stream[F, A]] =
    collectionExists(collection).compile.last map(_ getOrElse false) flatMap { exists =>
      if (exists) str.pure[F]
      else MonadResourceErr.raiseError(ResourceError.pathNotFound(collection.resourcePath))
    }

  def findAll(collection: Collection): F[Stream[F, BsonValue]] =
    withCollectionExists(
      collection,
      getCollection(collection)
        .find[BsonValue]
        .batchSize(batchSize)
        .toStream(batchSize))

  def aggregate(collection: Collection, aggs: List[BsonDocument]): F[Stream[F, BsonValue]] =
    withCollectionExists(
      collection,
      getCollection(collection)
        .aggregate[BsonValue](aggs)
        .allowDiskUse(true)
        .batchSize(batchSize)
        .toStream(batchSize))

  def evaluateImpl(collection: Collection, aggs: List[BsonDocument])
      : F[Either[Throwable, Stream[F, BsonValue]]] = {

    val aggregated =
      aggregate(collection, aggs)

    val hd: F[Either[Throwable, Option[BsonValue]]] =
      F.attempt(aggregated flatMap (_.head.compile.last))

    hd.flatMap(_.as(aggregated).sequence)
  }

  def evaluate(collection: Collection, stages: ScalarStages, offset: Option[MongoOffset])
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

  private def evaluateNoPushdownWithOffset(collection: Collection, stages: ScalarStages, offset: MongoOffset)
      : F[(ScalarStages, Stream[F, BsonValue])] =
    offsetInterpret(offset) match {
      case Some(pipelineDocs) =>
        evaluateImpl(collection, pipelineDocs) flatMap {
          case Right(results) => (stages, results).pure[F]
          case Left(_) => MonadResourceErr[F].raiseError(ResourceError.seekFailed(collection.resourcePath, "Could not execute $match stage"))
        }
      case None =>
        MonadResourceErr[F].raiseError(ResourceError.seekFailed(collection.resourcePath, "Could not compile offset into $match stage"))
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

  def mkClient[F[_]: ContextShift](
      config: MongoConfig,
      blocker: Blocker)(
      implicit F: Sync[F])
      : Resource[F, MongoClient] =
    Settings[F](config, blocker) flatMap { clusterSettings =>
      Resource(for {
        conn <- F.delay { new ConnectionString(config.connectionString) }

        rawSettings <- F.delay {
          val updateCluster: Block[ClusterSettings.Builder] =
            new Block[ClusterSettings.Builder] {
              def apply(t: ClusterSettings.Builder): Unit = {
                val _ = t.applySettings(clusterSettings); ()
              }
            }

          MongoClientSettings
            .builder
            .applyConnectionString(conn)
            .applyToClusterSettings(updateCluster)
            .build
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

    def buildInfo(client: MongoClient): F[Document] =
      client
        .getDatabase("admin")
        .runCommand[Document](Document("buildInfo" -> 1))
        .toStream[IO](1)
        .compile
        .lastOrError

    def getVersionString(doc: Document): Option[String] =
      Either.catchOnly[ClassCastException](doc.getString("version")).toOption

    def mkVersion(parts: Array[String]): Option[Version] = for {
      majorString <- parts.lift(0)
      minorString <- parts.lift(1)
      patchString <- parts.lift(2)
      major <- Either.catchOnly[NumberFormatException](majorString.toInt).toOption
      minor <- Either.catchOnly[NumberFormatException](minorString.toInt).toOption
      patch <- Either.catchOnly[NumberFormatException](patchString.toInt).toOption
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
        batchSize = scala.math.max(1, config.batchSize)
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
