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

import cats.effect._
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.option._

import eu.timepit.refined.auto._

import fs2.Stream

import quasar.api.datasource.DatasourceType
import quasar.api.resource.{ResourcePath, ResourceName, ResourcePathType}
import quasar.connector.{MonadResourceErr, QueryResult, ResourceError}
import quasar.connector.datasource.LightweightDatasource
import quasar.physical.mongo.decoder.qdataDecoder
import quasar.physical.mongo.MongoResource.{Database, Collection}
import quasar.qscript.InterpretedRead

import shims._

class MongoDataSource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](mongo: Mongo[F])
  extends LightweightDatasource[F, Stream[F, ?], QueryResult[F]] {

  val kind = MongoDataSource.kind

  override def evaluate(iRead: InterpretedRead[ResourcePath]): F[QueryResult[F]] = {
    val path = iRead.path
    val errored =
      MonadResourceErr.raiseError(ResourceError.pathNotFound(path))
    val interpretation: Interpretation = mongo.interpreter.interpret(iRead.instructions)

    val fStream = path match {
      case ResourcePath.Root => errored
      case ResourcePath.Leaf(file) => MongoResource.ofFile(file) match {
        case None => errored
        case Some(Database(_)) => errored
        case Some(collection@Collection(_, _)) =>
          if (interpretation.aggregators.isEmpty) mongo.findAll(collection)
          else mongo.aggregate(collection, interpretation.aggregators) map (_ map interpretation.resultMapper)
      }
    }
    fStream.map(stream => QueryResult.parsed(qdataDecoder, stream, iRead.instructions))
  }

  override def pathIsResource(path: ResourcePath): F[Boolean] = path match {
    case ResourcePath.Root => false.pure[F]
    case ResourcePath.Leaf(file) => MongoResource.ofFile(file) match {
      case Some(Database(_)) => false.pure[F]
      case Some(coll@Collection(_, _)) => mongo.collectionExists(coll).compile.last.map(_ getOrElse false)
      case None => false.pure[F]
    }
  }

  override def prefixedChildPaths(prefixPath: ResourcePath)
    : F[Option[Stream[F, (ResourceName, ResourcePathType)]]] = prefixPath match {

    case ResourcePath.Root =>
      mongo.databases.map(x => (ResourceName(x.name), ResourcePathType.prefix)).some.pure[F]

    case ResourcePath.Leaf(file) => MongoResource.ofFile(file) match {
      case None => none.pure[F]
      case Some(Collection(_, _)) => none.pure[F]
      case Some(db@Database(_)) => {
        val dbExists: F[Boolean] = mongo.databaseExists(db).compile.last.map(_.getOrElse(false))

        dbExists.map(exists => {
          if (exists) {
            mongo.collections(db).map(x => (ResourceName(x.name), ResourcePathType.leafResource)).some
          } else {
            none
          }
        })
      }
    }
  }
}

object MongoDataSource {
  val kind: DatasourceType = DatasourceType("mongo", 1L)
}
