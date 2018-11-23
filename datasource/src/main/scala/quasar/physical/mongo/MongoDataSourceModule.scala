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

import quasar.api.datasource.{DatasourceError, DatasourceType}
import quasar.api.datasource.DatasourceError.{InitializationError}
import quasar.api.resource.ResourcePath
import quasar.connector.{LightweightDatasourceModule, MonadResourceErr, Datasource, QueryResult}
import quasar.Disposable

import scala.concurrent.ExecutionContext

import argonaut._
import cats.effect.{ConcurrentEffect, Timer, ContextShift}
import cats.syntax.applicative._
import cats.syntax.functor._
import fs2.Stream
import scalaz.{NonEmptyList, \/}
import scalaz.syntax.either._

object MongoDataSourceModule extends LightweightDatasourceModule {
  type DS[F[_]] = Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]
  type Result[F[_]] = Disposable[F, DS[F]]
  type Error = InitializationError[Json]
  type ResultOrError[F[_]] = InitializationError[Json] \/ Result[F]

  private def mkError[F[_]](config: Json, msg: String): ResultOrError[F] =
    DatasourceError
      .invalidConfiguration[Json, Error](kind, config, NonEmptyList(msg))
      .left[Result[F]]

  @SuppressWarnings(Array("org.wartremover.warts.ImplicitParameter"))
  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer]
    (config: Json)
    (implicit ec: ExecutionContext)
    : F[ResultOrError[F]] =

    config.as[MongoConfig].result match {
      case Left((msg, _)) =>
        mkError(config, msg).pure[F]
      case Right(mongoConfig) => {
        Mongo(mongoConfig).attempt.compile.last.map (_ match {
          case None =>
            mkError(config, "Incorrect connection string")
          case Some(Left(e)) =>
            mkError(config, e.getMessage())
          case Some(Right(client)) => {
            val ds: DS[F] = new MongoDataSource(client)
            Disposable(ds, client.close).right
          }
        })
      }
    }

  def kind: DatasourceType = MongoDataSource.kind

  def sanitizeConfig(config: Json): Json = MongoConfig.sanitize(config)
}
