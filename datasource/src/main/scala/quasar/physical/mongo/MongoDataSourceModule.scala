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

import quasar.connector.{LightweightDatasourceModule, MonadResourceErr, Datasource, QueryResult}
import quasar.api.datasource.{DatasourceError, DatasourceType}
import quasar.api.datasource.DatasourceError.{InitializationError}
import quasar.api.resource.ResourcePath
import quasar.Disposable

import argonaut._
import fs2.Stream
import cats.syntax.functor._
import cats.effect.{ConcurrentEffect, Timer, ContextShift}
import scalaz.{NonEmptyList, \/}
import scalaz.syntax.either._
import cats.syntax.applicative._
import scala.concurrent.ExecutionContext

object MongoDataSourceModule extends LightweightDatasourceModule {
  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr : Timer]
    (config: Json)
    (implicit ec: ExecutionContext)
    : F[InitializationError[Json] \/ Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]]] =

    config.as[MongoConfig].result match {
      case Left((msg, _)) =>
        DatasourceError.invalidConfiguration[Json, InitializationError[Json]](kind, config, NonEmptyList(msg))
          .left[Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]]]
          .pure[F]

      case Right(mongoConfig) => {
        Mongo(mongoConfig).attempt.compile.last.map (_ match {
          case None =>
            DatasourceError
              .invalidConfiguration[Json, InitializationError[Json]](kind, config, NonEmptyList("Incorrect connection string"))
              .left[Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]]]
          case Some(Left(e)) =>
            DatasourceError
              .invalidConfiguration[Json, InitializationError[Json]](kind, config, NonEmptyList(e.getMessage()))
              .left[Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]]]
          case Some(Right(client)) => {
            val ds: Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]] = new MongoDataSource(client)
            val disposable = Disposable(ds, client.close)
            disposable.right[InitializationError[Json]]
          }
        })
      }
    }

  def kind: DatasourceType = MongoDataSource.kind

  def sanitizeConfig(config: Json): Json = MongoConfig.sanitize(config)
}
