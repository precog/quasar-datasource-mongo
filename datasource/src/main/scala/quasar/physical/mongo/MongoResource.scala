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

import cats.kernel.Eq
import cats.syntax.option._
import quasar.contrib.pathy.AFile
import pathy.Path
import shims._

sealed trait MongoResource

object MongoResource {
  def database(name: String): MongoResource = Database(name)
  def collection(database: Database, name: String): MongoResource = Collection(database, name)
  def apply(a: MongoResource): MongoResource = a

  implicit val equal: Eq[MongoResource] = Eq.fromUniversalEquals

  def ofFile(file: AFile): Option[MongoResource] = {
    Path.peel(file) match {
      case Some((firstLayer, eitherName)) => Path.peel(firstLayer) match {
        case None => Database(printName(eitherName.asCats)).some
        case Some((secondLayer, eitherDb)) => Path.peel(secondLayer) match {
          case Some(_) => none
          case None => Collection(Database(printName(eitherDb.asCats)), printName(eitherName.asCats)).some
        }
      }
      case None => none
    }
  }
  private def printName(p: Either[Path.DirName, Path.FileName]): String = p match {
    case Right(Path.FileName(n)) => n
    case Left(Path.DirName(n)) => n
  }

}

final case class Database(name: String) extends MongoResource
final case class Collection(database: Database, name: String) extends MongoResource
