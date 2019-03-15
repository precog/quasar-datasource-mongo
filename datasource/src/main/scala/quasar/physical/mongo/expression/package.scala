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

import matryoshka.data.Fix

import org.bson._

import scalaz.{Scalaz, MonadPlus}, Scalaz._
import scalaz.syntax._

package object expression {
  type ExprF[A] = Compiler.ExprF[A]
  type Expr = Fix[ExprF]

  type Step = Projection.Step
  type Field = Step.Field
  type Index = Step.Index

  val Step: Projection.Step.type = Projection.Step
  val Field: Step.Field.type = Step.Field
  val Index: Step.Index.type = Step.Index

  val O: Optics.FullOptics[Fix[Compiler.ExprF], Fix[Compiler.ExprF], Compiler.ExprF] = Optics.fullT[Fix, Compiler.ExprF]

  type Pipe = Pipeline[Expr]

  def compilePipeline[F[_]: MonadPlus](version: Version, uuid: String, pipes: List[Pipe]): F[List[BsonDocument]] =
    Compiler.compilePipeline[Fix, F](version, uuid, pipes)

  def compilePipe[F[_]: MonadPlus](version: Version, uuid: String, pipe: Pipe): F[List[BsonDocument]] =
    Compiler.eraseCustomPipeline(uuid, pipe) foldMapM { x => Compiler.compilePipe[Fix, F](version, uuid, x) map (List(_)) }

  def mapProjection(mp: Mapper)(pipe: Pipe): Pipe =
    pipe map Compiler.mapProjection(Mapper.projection(mp))

  def missing(key: String): Expr = Compiler.missing[Fix, ExprF](key)
  def missingKey(key: String): Expr = Compiler.missingKey[Fix, ExprF](key)
}
