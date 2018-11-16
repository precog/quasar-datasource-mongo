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
import org.specs2.mutable.Specification
import org.mongodb.scala._
import org.bson._

import cats.effect._
import scala.concurrent.ExecutionContext
import fs2.Stream
import fs2.concurrent._
import quasar.connector.ResourceError
import java.util.regex._

class Test extends Specification {
  def observableAsStream[F[_], A]
    (obs: Observable[A])
    (implicit F: ConcurrentEffect[F], cs: ContextShift[F]): Stream[F, A]  = {
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

  "ololo" >> {
    true
  }
  "connect to mongo" >> {
    implicit val ec: ExecutionContext = ExecutionContext.global
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)
    implicit val timer: Timer[IO] = IO.timer(ec)
    implicit def oasa[A]
      (obs: Observable[A])
      (implicit F: ConcurrentEffect[IO], cs: ContextShift[IO]): Stream[IO, A]  = {
        observableAsStream(obs)
    }

    val mongoClient: MongoClient = MongoClient()
    val db = mongoClient.getDatabase("data")


    val observable: Observable[String] = db.listCollectionNames()

//    scala.Predef.println(observableAsStream[IO, Document](mongoClient.listDatabases()).compile.toList.unsafeRunSync())
//    scala.Predef.println(observable.compile.toList.unsafeRunSync().length)

    scala.Predef.println(db)

    val wrongClient = MongoClient() //("mongodb://wrong")
/*    wrongClient.listDatabaseNames().subscribe(new Observer[String] {
      override def onNext(res: String): Unit = {
        scala.Predef.println("next")
      }
      override def onError(e: Throwable): Unit = {
        scala.Predef.println("error")
      }
      override def onComplete(): Unit = {
        scala.Predef.println("complete")
      }

      override def onSubscribe(sub: Subscription) = {
        scala.Predef.println("subscribed")
        sub.request(1)
        scala.Predef.println(sub.isUnsubscribed())
//        super.onSubscribe(sub)
      }
    })
 */
    val res = wrongClient.listDatabaseNames().attempt.compile.toList.unsafeRunSync()
//    val res = wrongClient.getDatabase("OLOLOLO").runCommand[Document](Document("ping" -> 1)).attempt.compile.toList.unsafeRunSync()
    scala.Predef.println(res)
//    scala.Predef.println(observableAsStream[IO, String](wrongClient.listDatabases()).attempt.take(1).compile.toList.unsafeRunSync())
//    scala.Predef.println(wrongDb.listCollectionNames().attempt.compile.toList.unsafeRunSync().length)
    scala.Predef.println(db)

    scala.Predef.println(wrongClient.getDatabase("data").getCollection("orders").find[BsonValue]().take(10).compile.toList.unsafeRunSync())
    val br = new BsonRegularExpression("[0-9]", "gi")
    val rg = Pattern.compile("[0-9]", Pattern.CASE_INSENSITIVE|Pattern.CANON_EQ)
    scala.Predef.println(br.toString())
    scala.Predef.println(rg.toString())
    true
  }
}
