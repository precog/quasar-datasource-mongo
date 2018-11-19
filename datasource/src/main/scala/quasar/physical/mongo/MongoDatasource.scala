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

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.{MonadResourceErr, QueryResult}
import quasar.connector.datasource._

import cats.effect._
import cats.syntax.applicative._
import cats.syntax.option._
import eu.timepit.refined.auto._
import fs2.Stream
import eu.timepit.refined.auto._
import quasar.physical.mongo.decoder._
import shims._

class MongoDataSource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
  mongo: Mongo[F]
  )
  extends LightweightDatasource[F, Stream[F, ?], QueryResult[F]] {

  val kind = MongoDataSource.kind

  override def evaluate(path: ResourcePath): F[QueryResult[F]] = path match {
    case ResourcePath.Root => QueryResult.parsed(qdataDecoder, Stream.empty.covary[F]).pure[F]
    case ResourcePath.Leaf(file) => MongoResource.ofFile(file) match {
      case None => QueryResult.parsed(qdataDecoder, Stream.empty.covary[F]).pure[F]
      case Some(Database(_)) => QueryResult.parsed(qdataDecoder, Stream.empty.covary[F]).pure[F]
      case Some(collection@Collection(_, _)) => QueryResult.parsed(qdataDecoder, mongo.findAll(collection)).pure[F]
    }
  }

  override def pathIsResource(path: ResourcePath): F[Boolean] = path match {
    case ResourcePath.Root => false.pure[F]
    case ResourcePath.Leaf(file) => MongoResource.ofFile(file) match {
      case Some(Database(_)) => false.pure[F]
      case Some(coll@Collection(_, _)) => mongo.collectionExists(coll).compile.lastOrError
      case None => false.pure[F]
    }
  }

  override def prefixedChildPaths(prefixPath: ResourcePath)
    : F[Option[Stream[F, (ResourceName, ResourcePathType)]]] = prefixPath match {
    case ResourcePath.Root => {
      mongo.databases.map(_ match {
        case Database(name) => (ResourceName(name), ResourcePathType.prefix)
      }).some.pure[F]
    }
    case ResourcePath.Leaf(file) => MongoResource.ofFile(file) match {
      case None => none.pure[F]
      case Some(Collection(_, _)) => none.pure[F]
      case Some(db@Database(_)) => {
        mongo.collections(db).map(_ match {
          case Collection(_, colName) => (ResourceName(colName), ResourcePathType.leafResource)
        }).some.pure[F]
      }
    }
  }
}

object MongoDataSource {
  val kind: DatasourceType = DatasourceType("mongo", 1L)
}
