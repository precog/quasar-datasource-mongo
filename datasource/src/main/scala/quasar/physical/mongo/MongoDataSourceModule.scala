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
import quasar.api.datasource.{DatasourceError, DatasourceType}, DatasourceError.{ConfigurationError, InitializationError}
import quasar.concurrent._
import quasar.connector.{ByteStore, MonadResourceErr, ExternalCredentials}
import quasar.connector.datasource.{LightweightDatasourceModule, Reconfiguration}

import java.util.UUID
import scala.concurrent.ExecutionContext

import argonaut._

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.kernel.Hash
import cats.implicits._

import scalaz.NonEmptyList

object MongoDataSourceModule extends LightweightDatasourceModule {
  type Error = InitializationError[Json]

  private def mkInvalidCfgError[F[_]](config: Json, msg: String): Error =
    DatasourceError.invalidConfiguration[Json, Error](
      kind,
      sanitizeConfig(config),
      NonEmptyList(msg))

  @SuppressWarnings(Array("org.wartremover.warts.ImplicitParameter"))
  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A: Hash](
      config: Json,
      rateLimiter: RateLimiting[F, A],
      byteStore: ByteStore[F],
      getAuth: UUID => F[Option[ExternalCredentials[F]]])(
      implicit ec: ExecutionContext)
      : Resource[F, Either[Error, LightweightDatasourceModule.DS[F]]] =
    config.as[MongoConfig].result match {
      case Left((msg, _)) =>
        mkInvalidCfgError[F](config, msg)
          .asLeft[LightweightDatasourceModule.DS[F]]
          .pure[Resource[F, ?]]

      case Right(mongoConfig) =>
        val mongoResource = Resource.suspend(Sync[F].delay(Blocker.cached[F]("mongo-datasource").flatMap(Mongo(mongoConfig, _))))
        MongoDataSource(mongoResource).asRight[Error].pure[Resource[F, ?]]
    }

  def kind: DatasourceType = MongoDataSource.kind

  def sanitizeConfig(config: Json): Json = MongoConfig.sanitize(config)

   def migrateConfig[F[_]: Sync](config: Json): F[Either[ConfigurationError[Json], Json]] =
     Sync[F].pure(Right(config))

  def reconfigure(original: Json, patch: Json): Either[ConfigurationError[Json], (Reconfiguration, Json)] =
    Right((Reconfiguration.Reset, patch))
}
