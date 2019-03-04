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

package quasar.physical.mongo.expression

import slamdata.Predef._

import matryoshka._
import matryoshka.implicits._

import monocle._

import org.bson._

import quasar.physical.mongo.Version

import scalaz.{Const, Coproduct, Scalaz, MonadPlus, ApplicativePlus}, Scalaz._
import scalaz.syntax._

import Pipeline._

object Compiler {
  type CoreOp[A] = Coproduct[Op, Core, A]
  type ExprF[A] = Coproduct[Const[Projection, ?], CoreOp, A]

  def compilePipeline[T[_[_]]: BirecursiveT, F[_]: MonadPlus](
      version: Version,
      pipes: List[Pipeline[T[ExprF]]])
      : F[List[BsonDocument]] =
    pipes.foldMapM { x => compilePipe[T, F](version, x) map (List(_)) }

  def toCoreOp[T[_[_]]: BirecursiveT](pipe: MongoPipeline[T[ExprF]]): T[CoreOp] =
    optimize(compileProjections(pipelineObjects(pipe)))

  def compilePipe[T[_[_]]: BirecursiveT, F[_]: MonadPlus](
      version: Version,
      customPipe: Pipeline[T[ExprF]])
      : F[BsonDocument] = {
    val pipe = eraseCustomPipeline(customPipe)
    if (version < pipeMinVersion(pipe)) MonadPlus[F].empty
    else {
      def transformM(op: Op[T[Core]]): F[Core[T[Core]]] =
        if (version < Op.opMinVersion(op)) MonadPlus[F].empty
        else opsToCore[T](op).point[F]

      toCoreOp(pipe)
        .transCataM[F, T[Core], Core](_.run.fold(transformM, MonadPlus[F].point(_)))
        .map(coreToBson[T](_))
        .flatMap(mbBsonDocument[F])
    }
  }

  def mbBsonDocument[F[_]: ApplicativePlus](inp: BsonValue): F[BsonDocument] = inp match {
    case x: BsonDocument => x.point[F]
    case _ => ApplicativePlus[F].empty
  }

  def eraseCustomPipeline[T[_[_]]: BirecursiveT](
      pipeline: Pipeline[T[ExprF]])
      : MongoPipeline[T[ExprF]] = {

    val O = Optics.fullT[T, ExprF]

    pipeline match {
      case NotNull(fld) => $match(O.obj(Map(fld -> O.$ne(O.nil()))))
      case $project(obj) => $project(obj)
      case $match(obj) => $match(obj)
      case $unwind(a, i) => $unwind(a, i)
    }
  }


  def pipelineObjects[T[_[_]]: BirecursiveT](pipe: MongoPipeline[T[ExprF]]): T[ExprF] = {
    val O = Optics.fullT[T, ExprF]
    pipe match {
      case $match(mp) =>
        O.obj(Map("$match" -> mp))
      case $project(mp) =>
        O.obj(Map("$project" -> O.obj(mp)))
      case $unwind(path, arrayIndex) =>
        O.obj(Map("$unwind" -> O.obj(Map(
          "path" -> O.string("$" concat path),
          "includeArrayIndex" -> O.string(arrayIndex),
          "preserveNullAndEmptyArrays" -> O.bool(true)))))
    }
  }

  def unfoldProjection[T[_[_]]: BirecursiveT](prj: Projection): T[CoreOp] = {
    trait GroupedSteps
    final case class IndexGroup(i: Int) extends GroupedSteps
    final case class FieldGroup(s: List[String]) extends GroupedSteps
    final case class IndexedAccess(i: Int) extends GroupedSteps

    def groupSteps(prj: Projection): List[GroupedSteps] = {
      val accum = prj.steps.foldl ((List[String](), List[GroupedSteps]())) {
        case (fldAccum, accum) => {
          case Field(s) => (s :: fldAccum, accum)
          case Index(i) => (List(), IndexGroup(i) :: FieldGroup(fldAccum.reverse) :: accum)
        }
      }
      accum._1 match {
        case List() => accum._2.reverse
        case x => (FieldGroup(x.reverse) :: accum._2).reverse
      }
    }
    type Elem = (GroupedSteps, Int)

    val O = Optics.coreOp(Prism.id[CoreOp[List[Elem]]])

    val ψ: Coalgebra[CoreOp, List[Elem]] = {
      case List() =>
        O.string("$$ROOT")
      case (FieldGroup(hd :: tail), _) :: List() =>
        O.string(tail.foldl("$" concat hd) { accum => s => accum concat "." concat s  })
      case (hd, levelIx) :: tl => hd match {
        case IndexedAccess(i) =>
          O.$arrayElemAt(tl, i)
        case IndexGroup(i) =>
          val level = "level" concat levelIx.toString
          val vars: Map[String, List[Elem]] = Map(level -> tl)
          val expSteps = IndexedAccess(i)
          val exp = List[Elem]((expSteps, 0), (FieldGroup(List("$" concat level)), 1))
          O.$let(vars, exp)
        case FieldGroup(steps) =>
          val level = "level".concat(levelIx.toString)
          val vars = Map(level -> tl)
          val expSteps: GroupedSteps = FieldGroup("$".concat(level) :: steps)
          val exp = List((expSteps, 0))
          O.$let(vars, exp)
      }
    }
    groupSteps(prj).zipWithIndex.reverse.ana[T[CoreOp]](ψ)
  }

  def compileProjections[T[_[_]]: BirecursiveT](inp: T[ExprF]): T[CoreOp] = {
    def τ(inp: Const[Projection, T[CoreOp]]): CoreOp[T[CoreOp]] =
      unfoldProjection(inp.getConst).project
    inp.transCata[T[CoreOp]](_.run.fold(τ, (x => x)))
  }

  def opsToCore[T[_[_]]: BirecursiveT](inp: Op[T[Core]]): Core[T[Core]] = {
    val O = Optics.core(birecursiveIso[T[Core], Core].reverse.asPrism)
    inp match {
      case Op.Let(vars, in) => O._obj(Map("$let" -> O.obj(Map(
        "vars" -> O.obj(vars),
        "in" -> in))))
      case Op.Type(a) => O._obj(Map("$type"-> a))
      case Op.Eq(a) => O._obj(Map("$eq" -> O.array(a)))
      case Op.Or(a) => O._obj(Map("$or" -> O.array(a)))
      case Op.Exists(a) => O._obj(Map("$exists" -> a))
      case Op.Cond(a, b, c) => O._obj(Map("$cond" -> O.array(List(a, b, c))))
      case Op.Ne(a) => O._obj(Map("$ne" -> a))
      case Op.ObjectToArray(a) => O._obj(Map("$objectToArray" -> a))
      case Op.ArrayElemAt(a, ix) => O._obj(Map("$arrayElemAt" -> O.array(List(a, O.int(ix)))))
    }
  }

  def compileOps[T[_[_]]: BirecursiveT](inp: T[CoreOp]): T[Core] =
    inp.transCata[T[Core]](_.run.fold(opsToCore[T], (x => x)))

  def coreToBson[T[_[_]]: BirecursiveT](inp: T[Core]): BsonValue = {
    import scala.collection.JavaConverters._

    def ϕ: Algebra[Core, BsonValue] = {
      case Core.Null() =>
        new BsonNull()
      case Core.Int(i) =>
        new BsonInt32(i)
      case Core.String(s) =>
        new BsonString(s)
      case Core.Bool(b) =>
        new BsonBoolean(b)
      case Core.Array(as) =>
        new BsonArray(as.asJava)
      case Core.Object(mp) =>
        val elems = mp.toList map {
          case (key, v) => new BsonElement(key, v)
        }
        new BsonDocument(elems.asJava)
    }
    inp.cata[BsonValue](ϕ)
  }

  def optimize[T[_[_]]: BirecursiveT](inp: T[CoreOp]): T[CoreOp] = {
    orOpt(letOpt(inp))
  }

  def letOpt[T[_[_]]: BirecursiveT](inp: T[CoreOp]): T[CoreOp] = {
    val O = Optics.coreOp(Prism.id[CoreOp[T[CoreOp]]])
    val τ: CoreOp[T[CoreOp]] => CoreOp[T[CoreOp]] = {
      case O.$let(vars, in) =>
        if (vars.size /== 1) O.$let(vars, in)
        else in.project match {
          case O.string(str) =>
            vars.get(str.stripPrefix("$$")) map (_.project) match {
              case Some(O.string(x)) if x.stripPrefix("$") /== x =>
                O.string(x.concat(x))
              case _ =>
                O.$let(vars, in)
            }
          case _ => O.$let(vars, in)
        }
      case x => x
    }
    inp.transCata[T[CoreOp]](τ)
  }

  def orOpt[T[_[_]]: BirecursiveT](inp: T[CoreOp]): T[CoreOp] = {
    val O = Optics.coreOp(Prism.id[CoreOp[T[CoreOp]]])
    val τ: CoreOp[T[CoreOp]] => CoreOp[T[CoreOp]] = {
      case O.$or(lst) => lst match {
        case List() => O.bool(false)
        case List(a) => a.project
        case _ => O.$or(lst map (_.project) flatMap {
          case O.$or(as) => as
          case x => List(x.embed)
        })
      }
      case x => x
    }
    inp.transCata[T[CoreOp]](τ)
  }

  def mapProjection[T[_[_]]: BirecursiveT](f: Projection => Projection)(inp: T[ExprF]): T[ExprF] = {
    val O = Optics.full(Prism.id[ExprF[T[ExprF]]])
    val τ: ExprF[T[ExprF]] => ExprF[T[ExprF]] = {
      case O.projection(prj) => O.projection(f(prj))
      case x => x
    }
    inp.transCata[T[ExprF]](τ)
  }



}
