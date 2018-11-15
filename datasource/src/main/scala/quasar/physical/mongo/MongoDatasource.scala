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
import cats.kernel.Eq
import cats.syntax.applicative._
import cats.syntax.eq._
import cats.syntax.option._
import eu.timepit.refined.auto._
import fs2.Stream
import fs2.concurrent._
import org.mongodb.scala._
import eu.timepit.refined.auto._
import org.mongodb.scala._
import quasar.contrib.pathy.AFile
import pathy.Path
import shims._

class MongoDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
  mongoClient: MongoClient
  )
  extends LightweightDatasource[F, Stream[F, ?], QueryResult[F]] {
  val kind: DatasourceType = DatasourceType("mongo", 1L)

  override def evaluate(path: ResourcePath): F[QueryResult[F]] = {
    ???
  }

  override def pathIsResource(path: ResourcePath): F[Boolean] = path match {
    case ResourcePath.Root => false.pure[F]
    case ResourcePath.Leaf(file) => fileAsMongoResource(file) match {
      case Some(Db(_)) => false.pure[F]
      case Some(c@Coll(db, coll)) => collectionExists(c)
      case None => false.pure[F]
    }
  }

  override def prefixedChildPaths(prefixPath: ResourcePath)
    : F[Option[Stream[F, (ResourceName, ResourcePathType)]]] = prefixPath match {
    case ResourcePath.Root => {
      val names = MongoDatasource.observableAsStream(mongoClient.listDatabaseNames())
      val res: Stream[F, (ResourceName, ResourcePathType)] = names.map( name => {
        (ResourceName(name), ResourcePathType.prefix)
      })
      res.some.pure[F]
    }
    case ResourcePath.Leaf(file) => fileAsMongoResource(file) match {
      case None => none.pure[F]
      case Some(Coll(_, _)) => none.pure[F]
      case Some(db@Db(dbName)) => {
        val res: Stream[F, (ResourceName, ResourcePathType)] = collections(db).map(_ match {
          case Coll(_, collName) => {
            (ResourceName(collName), ResourcePathType.leafResource)
          }
        })
        res.some.pure[F]
      }
    }
  }

  sealed trait MongoResource
  object MongoResource {
    def db(name: String): MongoResource = Db(name)
    def coll(dbName: String, collName: String) = Coll(dbName, collName)
    def apply(a: MongoResource): MongoResource = a

    implicit val equal: Eq[MongoResource] = Eq.fromUniversalEquals
  }
  case class Db(name: String) extends MongoResource
  case class Coll(db: String, coll: String) extends MongoResource

  private def fileAsMongoResource(file: AFile): Option[MongoResource] = {
    Path.peel(file) match {
      case Some((firstLayer, eitherName)) => Path.peel(firstLayer) match {
        case None => Db(printName(eitherName.asCats)).some
        case Some((secondLayer, eitherDB)) => Path.peel(secondLayer) match {
          case Some(_) => none
          case None => Coll(printName(eitherDB.asCats), printName(eitherName.asCats)).some
        }
      }
    }
  }

  private def databaseExistenceSignal(db: Db): Stream[F, Boolean] = db match {
    case Db(name) =>
      MongoDatasource
        .observableAsStream(mongoClient.listDatabaseNames())
        .exists(_ == name)
  }
  private def collections(db: Db): Stream[F, Coll] = db match {
    case Db(name) => {
      for {
        dbExists <- databaseExistenceSignal(db)
        res <- if (dbExists) {collectionNames(db)} else {Stream.empty}
      } yield res
    }
  }
  private def collectionNames(db: Db): Stream[F, Coll] = db match {
    case Db(dbName) => {
      val db: MongoDatabase = mongoClient.getDatabase(dbName)
      MongoDatasource
        .observableAsStream(db.listCollectionNames())
        .map(colName => Coll(dbName, colName))
      }
  }
  private def collectionExists(coll: Coll): F[Boolean] = coll match {
    case Coll(dbName, _) => {
      collections(Db(dbName))
        .exists(MongoResource(_) === MongoResource(coll))
        .compile
        .lastOrError
    }
  }
  private def printName(p: Either[Path.DirName, Path.FileName]): String = p match {
    case Right(Path.FileName(n)) => n
    case Left(Path.DirName(n)) => n
  }
}

object MongoDatasource {
  def observableAsStream[F[_], A]
    (obs: Observable[A])
    (implicit F: ConcurrentEffect[F]/*, cs: ContextShift[F]*/): Stream[F, A]  = {
    def handler(cb: Either[Throwable, Option[A]] => Unit): Unit = {
      obs.subscribe(new Observer[A] {
        override def onNext(result: A): Unit = {
          cb(Right(Some(result)))
        }
        override def onError(e: Throwable): Unit = {
          cb(Left(e))
        }
        override def onComplete(): Unit = {
          cb(Right(None))
        }
      })
    }
    for {
      q <- Stream.eval(Queue.unbounded[F, Either[Throwable, Option[A]]])
      _ <- Stream.eval { F.delay(handler(r => F.runAsync(q.enqueue1(r))(_ => IO.unit).unsafeRunSync)) }
      res <- q.dequeue.rethrow.unNoneTerminate
     } yield res
  }

}
