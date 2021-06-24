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

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import fs2.Stream

import quasar.api.DataPathSegment
import quasar.api.datasource.DatasourceType
import quasar.api.push.OffsetPath
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.{MonadResourceErr, Offset, QueryResult, ResourceError, ResultData}
import quasar.connector.datasource._
import quasar.physical.mongo.decoder.qdataDecoder
import quasar.physical.mongo.MongoResource.{Collection, Database}
import quasar.qscript.InterpretedRead

final class MongoDataSource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
    rMongo: => Resource[F, Mongo[F]])
    extends LightweightDatasource[Resource[F, ?], Stream[F, ?], QueryResult[F]] {

  val kind = MongoDataSource.kind

  val loaders = NonEmptyList.of(Loader.Batch(BatchLoader.Seek(loader(_, _))))

  def pathIsResource(path: ResourcePath): Resource[F, Boolean] = path match {
    case ResourcePath.Root => false.pure[Resource[F, ?]]
    case ResourcePath.Leaf(file) => MongoResource.ofFile(file) match {
      case Some(Database(_)) => false.pure[Resource[F, ?]]
      case Some(coll@Collection(_, _)) =>
        rMongo.evalMap(_.collectionExists(coll))
      case None => false.pure[Resource[F, ?]]
    }
  }

  def prefixedChildPaths(prefixPath: ResourcePath)
      : Resource[F, Option[Stream[F, (ResourceName, ResourcePathType.Physical)]]] =
    prefixPath match {
      case ResourcePath.Root =>
        rMongo.map(_.databases.map(x => (ResourceName(x.name), ResourcePathType.prefix)).some)
      case ResourcePath.Leaf(file) => MongoResource.ofFile(file) match {
        case None => none.pure[Resource[F, ?]]
        case Some(coll@Collection(_, _)) =>
          rMongo.evalMap(_.collectionExists(coll)) map { exists =>
            if (exists) Stream.empty.some
            else none
          }
        case Some(db@Database(_)) =>
          for {
            mongo <- rMongo
            exists <- Resource.eval(mongo.databaseExists(db))
          } yield {
            if (exists)
              mongo.collections(db).map(x => (ResourceName(x.name), ResourcePathType.leafResource)).some
            else
              none
          }
      }
    }

  ////

  private def loader(iRead: InterpretedRead[ResourcePath], offset: Option[Offset]):
      Resource[F, QueryResult[F]] = {
    val path = iRead.path
    val errored =
      Resource.eval(MonadResourceErr.raiseError(ResourceError.pathNotFound(path)))

    val rStreamPair = path match {
      case ResourcePath.Root =>
        errored
      case ResourcePath.Leaf(file) => MongoResource.ofFile(file) match {
        case None =>
          errored
        case Some(Database(_)) =>
          errored
        case Some(collection@Collection(_, _)) =>
          for {
            mongo <- rMongo
            off <- Resource.eval(offset.traverse(mongoOffset(path, _)))
            res <- Resource.eval(mongo.evaluate(collection, iRead.stages, off))
          } yield res
      }
    }

    rStreamPair map {
      case (insts, stream) => QueryResult.parsed(qdataDecoder, ResultData.Continuous(stream), insts)
    }
  }

  private def mongoOffset(forResource: ResourcePath, offset: Offset): F[MongoOffset] = {
    type S = Either[String, Int]

    def supportedPath(path: OffsetPath): F[NonEmptyList[S]] = path traverse {
      case DataPathSegment.Field(n) => (Left(n): S).pure[F]
      case DataPathSegment.Index(i) => (Right(i): S).pure[F]

      case DataPathSegment.AllFields =>
        MonadResourceErr.raiseError[S](ResourceError.seekFailed(
          forResource,
          "Offset path containing 'all fields' is not supported."))

      case DataPathSegment.AllIndices =>
        MonadResourceErr.raiseError[S](ResourceError.seekFailed(
          forResource,
          "Offset path containing 'all indicies' is not supported."))
    }

    for {
      internalOffset <- offset match {
        case internal: Offset.Internal => internal.pure[F]
        case _ =>
          MonadResourceErr.raiseError[Offset.Internal](ResourceError.seekFailed(
            forResource,
            "External offsets are not supported"))
      }
      p <- supportedPath(internalOffset.path)
    } yield MongoOffset(p, internalOffset.value)
  }
}

object MongoDataSource {
  val kind: DatasourceType = DatasourceType("mongo", 1L)

  def apply[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      mongo: => Resource[F, Mongo[F]])
      : MongoDataSource[F] =
    new MongoDataSource(mongo)
}
