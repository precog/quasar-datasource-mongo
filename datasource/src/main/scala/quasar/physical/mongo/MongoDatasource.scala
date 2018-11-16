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
import qdata._
import qdata.QType._

import java.time._
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
import org.bson._
import quasar.contrib.pathy.AFile
import pathy.Path
import spire.math.Real
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import shims._

class MongoDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
  mongoClient: MongoClient
  )
  extends LightweightDatasource[F, Stream[F, ?], QueryResult[F]] {
  val kind: DatasourceType = DatasourceType("mongo", 1L)

  override def evaluate(path: ResourcePath): F[QueryResult[F]] = path match {
    case ResourcePath.Root => QueryResult.parsed(qdataDecoder, Stream.empty.covary[F]).pure[F]
    case ResourcePath.Leaf(file) => fileAsMongoResource(file) match {
      case None => QueryResult.parsed(qdataDecoder, Stream.empty.covary[F]).pure[F]
      case Some(Db(_)) => QueryResult.parsed(qdataDecoder, Stream.empty.covary[F]).pure[F]
      case Some(coll@Coll(_, _)) => QueryResult.parsed(qdataDecoder, everythingInCollection(coll)).pure[F]
    }
  }

  private def everythingInCollection(coll: Coll): Stream[F, BsonValue] = {
    for {
      colExists <- collectionExistsSignal(coll)
      res <- if (!colExists) { Stream.empty } else { coll match {
        case Coll(dbName, collName) => {
          val collection = mongoClient.getDatabase(dbName).getCollection(collName)
          MongoDatasource.observableAsStream(collection.find[BsonValue]())
        }
      }}
    } yield res
  }

  private val qdataDecoder: QDataDecode[BsonValue] = new QDataDecode[BsonValue] {
    override def tpe(bson: BsonValue): QType = bson.getBsonType() match {
      case BsonType.DOCUMENT => QObject
      case BsonType.ARRAY => QArray
      case BsonType.STRING => QString
      case BsonType.INT32 => QLong
      case BsonType.INT64 => QLong
      case BsonType.DOUBLE => QDouble
      case BsonType.DECIMAL128 => QReal
      case BsonType.BOOLEAN => QBoolean
      case BsonType.OBJECT_ID => QString
      case BsonType.DB_POINTER => QString
      case BsonType.BINARY => QString
      case BsonType.DATE_TIME => QOffsetDateTime
      case BsonType.SYMBOL => QString
      case BsonType.REGULAR_EXPRESSION => QString
      case BsonType.JAVASCRIPT => QString
      case BsonType.NULL => QNull
      case BsonType.TIMESTAMP => QOffsetDateTime
      case BsonType.UNDEFINED => QNull
      case BsonType.JAVASCRIPT_WITH_SCOPE => QString
      case BsonType.MAX_KEY => QString
      case BsonType.MIN_KEY => QString
      case BsonType.END_OF_DOCUMENT => QNull
    }

    type ArrayCursor = List[BsonValue]

    override def getArrayCursor(bson: BsonValue): ArrayCursor = bson match {
      case arr: BsonArray => {
        arr.getValues().asScala.toList
      }
    }
    override def getArrayAt(cursor: ArrayCursor): BsonValue = cursor match {
      case (hd :: _) => hd
      case _ => ???
    }
    override def hasNextArray(cursor: ArrayCursor): Boolean = cursor match {
      case (hd :: _) => false
      case _ => true
    }
    override def stepArray(cursor: ArrayCursor): ArrayCursor = cursor match {
      case (_ :: tl) => tl
      case _ => ???
    }

    type ObjectCursor = (List[String], BsonDocument)

    override def getObjectCursor(bson: BsonValue): ObjectCursor = bson match {
      case obj: BsonDocument => {
        val lst: List[String] = obj.keySet().toList
        (lst, obj)
      }
    }
    override def getObjectKeyAt(cursor: ObjectCursor): String = cursor match {
      case ((k :: _), obj) => k
      case _ => ???
    }
    override def getObjectValueAt(cursor: ObjectCursor): BsonValue = cursor match {
      case ((k :: _), obj) => obj.get(k)
      case _ => ???
    }
    override def hasNextObject(cursor: ObjectCursor): Boolean = cursor match {
      case (List(), _) => false
      case _ => true
    }
    override def stepObject(cursor: ObjectCursor): ObjectCursor = cursor match {
      case ((_ :: tail), obj) => (tail, obj)
      case _ => ???
    }
    override def getBoolean(bson: BsonValue): Boolean = bson match {
      case bool: BsonBoolean => bool.getValue()
    }
    override def getDouble(bson: BsonValue): Double = bson match {
      case num: BsonNumber => num.doubleValue()
    }
    override def getLong(bson: BsonValue): Long = bson match {
      case num: BsonNumber => num.longValue()
    }
    override def getReal(bson: BsonValue): Real = bson match {
      case num: BsonNumber => Real(num.decimal128Value().bigDecimalValue())
    }
    override def getString(bson: BsonValue): String = bson match {
      case str: BsonString => str.getValue()
      case objId: BsonObjectId => objId.getValue().toHexString()
      case pointer: BsonDbPointer => {
        pointer.getNamespace() ++ ":" ++ pointer.getId().toHexString()
      }
      case binary: BsonBinary => binary.getData().toString()
      case symbol: BsonSymbol => symbol.getSymbol()
      case regexp: BsonRegularExpression => regexp.getPattern()
      case js: BsonJavaScript => js.getCode()
      case maxKey: BsonMaxKey => maxKey.toString()
      case minKey: BsonMinKey => minKey.toString()
      case jsWithScope: BsonJavaScriptWithScope => jsWithScope.getCode()
    }

    override def getOffsetDateTime(bson: BsonValue): OffsetDateTime = bson match {
      case ts: BsonTimestamp => {
        Instant.ofEpochSecond(ts.getTime().longValue).atOffset(ZoneOffset.UTC)
      }
      case date: BsonDateTime => {
        Instant.ofEpochMilli(date.getValue()).atOffset(ZoneOffset.UTC)
      }
    }

    override def getInterval(a: BsonValue) = ???
    override def getLocalDate(a: BsonValue) = ???
    override def getLocalDateTime(a: BsonValue) = ???
    override def getLocalTime(a: BsonValue) = ???
    override def getMetaMeta(a: BsonValue) = ???
    override def getMetaValue(a: BsonValue) = ???
    override def getOffsetDate(a: BsonValue) = ???
    override def getOffsetTime(a: BsonValue) = ???

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
  final case class Db(name: String) extends MongoResource
  final case class Coll(db: String, coll: String) extends MongoResource


  private def fileAsMongoResource(file: AFile): Option[MongoResource] = {
    Path.peel(file) match {
      case Some((firstLayer, eitherName)) => Path.peel(firstLayer) match {
        case None => Db(printName(eitherName.asCats)).some
        case Some((secondLayer, eitherDB)) => Path.peel(secondLayer) match {
          case Some(_) => none
          case None => Coll(printName(eitherDB.asCats), printName(eitherName.asCats)).some
        }
      }
      case None => none
    }
  }

  private def databaseExistenceSignal(db: Db): Stream[F, Boolean] = db match {
    case Db(name) =>
      MongoDatasource
        .observableAsStream(mongoClient.listDatabaseNames())
        .exists(_ == name)
  }
  private def collections(db: Db): Stream[F, Coll] = db match {
    case Db(dbName) => {
      for {
        dbExists <- databaseExistenceSignal(db)
        res <- if (!dbExists) {Stream.empty} else {
          val mDb: MongoDatabase = mongoClient.getDatabase(dbName)
          MongoDatasource
            .observableAsStream(mDb.listCollectionNames())
            .map(colName => Coll(dbName, colName))
        }
      } yield res
    }
  }
  private def collectionExistsSignal(coll: Coll): Stream[F, Boolean] = coll match {
    case Coll(dbName, _) =>
      collections(Db(dbName))
        .exists(MongoResource(_) === MongoResource(coll))
  }
  private def collectionExists(coll: Coll): F[Boolean] =
    collectionExistsSignal(coll).compile.lastOrError

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
