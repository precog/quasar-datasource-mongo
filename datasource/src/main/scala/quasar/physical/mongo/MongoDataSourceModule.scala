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

import quasar.RateLimiting
import quasar.api.datasource.{DatasourceError, DatasourceType}, DatasourceError.InitializationError
import quasar.{concurrent => qt}
import quasar.connector.{ByteStore, MonadResourceErr}
import quasar.connector.datasource.LightweightDatasourceModule
import quasar.physical.mongo.Mongo.{MongoAccessDenied, MongoConnectionFailed}

import scala.concurrent.ExecutionContext

import argonaut._

import cats.ApplicativeError
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Timer}
import cats.kernel.Hash
import cats.syntax.applicative._
import cats.syntax.either._

import scalaz.NonEmptyList

object MongoDataSourceModule extends LightweightDatasourceModule {
  type Error = InitializationError[Json]

  private lazy val blocker: Blocker = qt.Blocker.cached("mongo-datasource")

  private def mkInvalidCfgError[F[_]](config: Json, msg: String): Error =
    DatasourceError.invalidConfiguration[Json, Error](
      kind,
      sanitizeConfig(config),
      NonEmptyList(msg))

  private def mkError[F[_]](config: Json, throwable: Throwable): Error =
    throwable match {
      case MongoConnectionFailed((ex, _)) =>
        DatasourceError.ConnectionFailed(kind, sanitizeConfig(config), ex)

      case MongoAccessDenied((_, detail)) =>
        DatasourceError.AccessDenied(kind, sanitizeConfig(config), detail)

      case t =>
        mkInvalidCfgError(config, t.getMessage)
    }

  @SuppressWarnings(Array("org.wartremover.warts.ImplicitParameter"))
  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A: Hash](
      config: Json,
      rateLimiter: RateLimiting[F, A],
      byteStore: ByteStore[F])(
      implicit ec: ExecutionContext)
      : Resource[F, Either[Error, LightweightDatasourceModule.DS[F]]] =
    config.as[MongoConfig].result match {
      case Left((msg, _)) =>
        mkInvalidCfgError[F](config, msg)
          .asLeft[LightweightDatasourceModule.DS[F]]
          .pure[Resource[F, ?]]

      case Right(mongoConfig) =>
        ApplicativeError[Resource[F, ?], Throwable]
          .attempt(Mongo(mongoConfig, blocker))
          .map(_.bimap(mkError(config, _), new MongoDataSource(_): LightweightDatasourceModule.DS[F]))
    }

  def kind: DatasourceType = MongoDataSource.kind

  def sanitizeConfig(config: Json): Json = MongoConfig.sanitize(config)
}
