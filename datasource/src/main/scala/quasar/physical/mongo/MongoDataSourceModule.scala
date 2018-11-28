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

import argonaut._

import cats.effect.{ConcurrentEffect, Timer, ContextShift}
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.functor._

import fs2.Stream

import quasar.api.resource.ResourcePath
import quasar.api.datasource.{DatasourceError, DatasourceType}, DatasourceError.InitializationError
import quasar.connector.{LightweightDatasourceModule, MonadResourceErr, Datasource, QueryResult}
import quasar.Disposable

import scala.concurrent.ExecutionContext

import scalaz.syntax.either._
import scalaz.{NonEmptyList, \/}

object MongoDataSourceModule extends LightweightDatasourceModule {
  type DS[F[_]] = Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]
  type Result[F[_]] = Disposable[F, DS[F]]
  type Error = InitializationError[Json]
  type ErrorOrResult[F[_]] = Error \/ Result[F]

  private def mkError[F[_]](config: Json, msg: String): ErrorOrResult[F] =
    DatasourceError
      .invalidConfiguration[Json, Error](kind, sanitizeConfig(config), NonEmptyList(msg))
      .left[Result[F]]

  @SuppressWarnings(Array("org.wartremover.warts.ImplicitParameter"))
  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer]
    (config: Json)
    (implicit ec: ExecutionContext)
    : F[ErrorOrResult[F]] =

    config.as[MongoConfig].result match {
      case Left((msg, _)) =>
        mkError(config, msg).pure[F]
      case Right(mongoConfig) => {
        Mongo(mongoConfig).attempt.map { _ match {
          case Left(e) => mkError(config, e.getMessage)
          case Right(disposableClient) =>
            disposableClient.map(client => (new MongoDataSource(client): DS[F])).right[Error]
        }}
    }}

  def kind: DatasourceType = MongoDataSource.kind

  def sanitizeConfig(config: Json): Json = MongoConfig.sanitize(config)
}
