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
import quasar.physical.mongo.json._
import quasar.Disposable

import argonaut._, Argonaut._
import fs2.Stream
import cats.syntax.functor._
import cats.effect.{ConcurrentEffect, Timer, ContextShift}
import scalaz.{NonEmptyList, \/}
import scalaz.syntax.either._
import cats.syntax.applicative._
import scala.concurrent.ExecutionContext
import org.mongodb.scala._
import com.mongodb.ConnectionString

object MongoDataSourceModule extends LightweightDatasourceModule {
  def lightweightDatasource[F[_]](config: Json)
    (implicit F: ConcurrentEffect[F], cs: ContextShift[F], mre: MonadResourceErr[F], t: Timer[F], ec: ExecutionContext)
    : F[InitializationError[Json] \/ Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]]] =
    config.as[MongoConfig].result match {
      case Left((msg, _)) =>
        DatasourceError.invalidConfiguration[Json, InitializationError[Json]](kind, config, NonEmptyList(msg))
          .left[Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]]]
          .pure[F]
      case Right(MongoConfig(connectionString)) => {
        def checkClient(cli: MongoClient): Stream[F, Unit] = {
          val observable: Observable[Document] = cli.getDatabase("admin").runCommand[Document](Document("ping" -> 1))
          MongoDataSource.observableAsStream(observable).map(_ => ())
        }

        val clientSignal = for {
          client <- Stream.eval(F.delay(MongoClient(connectionString)))
          _ <- checkClient(client)
        } yield client

        clientSignal.attempt.compile.last.map (_ match {
          case None => {
            DatasourceError
              .invalidConfiguration[Json, InitializationError[Json]](kind, config, NonEmptyList("Incorrect connection string"))
              .left[Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]]]
          }
          case Some(Left(e)) =>
            DatasourceError
              .invalidConfiguration[Json, InitializationError[Json]](kind, config, NonEmptyList(e.getMessage()))
              .left[Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]]]
          case Some(Right(client)) => {
            val ds: Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]] = new MongoDataSource(client)
            val disposable = Disposable(ds, F.delay(client.close()))
            disposable.right[InitializationError[Json]]
          }
        })
      }
    }


  def kind: DatasourceType = MongoDataSource.kind

  def sanitizeConfig(config: Json): Json = config.as[MongoConfig].result match {
    case Left(_) => config
    case Right(MongoConfig(value)) => {
      val connectionString = new ConnectionString(value)
      val userName = connectionString.getUsername()
      val password = connectionString.getPassword()
      val newConnectionString = value.replace(userName ++ ":" ++ password ++ "@", "<REDACTED>:<REDACTED>@")
      MongoConfig(newConnectionString).asJson
    }
  }
}
