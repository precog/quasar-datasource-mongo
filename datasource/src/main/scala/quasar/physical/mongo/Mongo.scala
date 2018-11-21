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

import cats.effect.{ConcurrentEffect, IO}
import cats.syntax.eq._
import fs2.Stream
import fs2.concurrent._
import org.bson.{Document => _, _}
import org.mongodb.scala._
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.api.resource.{ResourceName, ResourcePath}
import slamdata.Predef._

class Mongo[F[_]: MonadResourceErr : ConcurrentEffect] private[Mongo](client: MongoClient) {
  import Mongo._

  val F: ConcurrentEffect[F] = ConcurrentEffect[F]

  def databases: Stream[F, Database] =
    observableAsStream(client.listDatabaseNames).map(Database(_))

  def databaseExists(database: Database): Stream[F, Boolean] =
    databases.exists(MongoResource(_) === MongoResource(database))

  def collections(database: Database): Stream[F, Collection] = database match {
    case Database(name) => for {
      dbExists <- databaseExists(database)
      res <- if (!dbExists) { Stream.empty } else {
        observableAsStream(client.getDatabase(name).listCollectionNames()).map(Collection(database, _))
      }
    } yield res
  }

  def collectionExists(collection: Collection): Stream[F, Boolean] = collection match {
    case Collection(db, _) => collections(db).exists(MongoResource(_) === MongoResource(collection))
  }

  def findAll(collection: Collection): Stream[F, BsonValue] = for {
    colExists <- collectionExists(collection)
    res <- collection match {
      case Collection(Database(db), coll) => if (colExists) {
        observableAsStream(client.getDatabase(db).getCollection(coll).find[BsonValue]())
      } else {
        Stream.raiseError(
          ResourceError.throwableP(
            ResourceError.pathNotFound(
              ResourcePath.root() / ResourceName(collection.database.name) / ResourceName(collection.name)
            )
          )
        )
      }
    }
  } yield res

  def close: F[Unit] = F.delay(client.close())

}
object Mongo {
  def observableAsStream[F[_], A](obs: Observable[A])(implicit F: ConcurrentEffect[F]): Stream[F, A] = {
    def handler(cb: Either[Throwable, Option[A]] => Unit): Unit = {
      obs.subscribe(new Observer[A] {
        override def onNext(result: A): Unit = cb(Right(Some(result)))
        override def onError(e: Throwable): Unit = cb(Left(e))
        override def onComplete(): Unit = cb(Right(None))
      })
    }
    for {
      q <- Stream.eval(Queue.unbounded[F, Either[Throwable, Option[A]]])
      _ <- Stream.eval { F.delay(handler(r => F.runAsync(q.enqueue1(r))(_ => IO.unit).unsafeRunSync)) }
      res <- q.dequeue.rethrow.unNoneTerminate
    } yield res
  }

  def apply[F[_]: ConcurrentEffect: MonadResourceErr](config: MongoConfig): Stream[F, Mongo[F]] = config match {
    case MongoConfig(conn) => {
      Stream.eval(ConcurrentEffect[F].delay(MongoClient(conn))).flatMap(client => {
        observableAsStream(
          client
            .getDatabase("admin")
            .runCommand[Document](Document("ping" -> 1)))
          .map(_ => new Mongo(client))
      })
    }
  }
}
