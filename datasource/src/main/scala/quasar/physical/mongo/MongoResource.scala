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

import cats.kernel.Eq
import cats.syntax.option._

import pathy.Path

import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.contrib.pathy.AFile

import scalaz.{\/, -\/, \/-}

sealed trait MongoResource {
  def resourcePath: ResourcePath
}

object MongoResource {
  final case class Database(name: String) extends MongoResource {
    override def resourcePath: ResourcePath =
      ResourcePath.root() / ResourceName(name)
  }
  final case class Collection(database: Database, name: String) extends MongoResource {
    override def resourcePath: ResourcePath =
      database.resourcePath / ResourceName(name)
  }

  implicit val eqMongoResource: Eq[MongoResource] = Eq.fromUniversalEquals
  implicit val eqCollection: Eq[Collection] = Eq.fromUniversalEquals
  implicit val eqDatabase: Eq[Database] = Eq.fromUniversalEquals

  def getDatabase(r: MongoResource): Database = r match {
    case d @ Database(_) => d
    case Collection(d, _) => d
  }

  def ofFile(file: AFile): Option[MongoResource] = {
    Path.peel(file) match {
      case Some((firstLayer, eitherName)) => Path.peel(firstLayer) match {
        case None => Database(printName(eitherName)).some
        case Some((secondLayer, eitherDb)) => Path.peel(secondLayer) match {
          case Some(_) => none
          case None => Collection(Database(printName(eitherDb)), printName(eitherName)).some
        }
      }
      case None => none
    }
  }


  private def printName(p: Path.DirName \/ Path.FileName): String = p match {
    case \/-(Path.FileName(n)) => n
    case -\/(Path.DirName(n)) => n
  }
}
