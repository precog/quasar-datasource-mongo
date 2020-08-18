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

package quasar.physical.mongo.expression

import slamdata.Predef._

import quasar.physical.mongo.Version
import quasar.contrib.iotac._

import cats._
import cats.data.Const
import cats.implicits._
import higherkindness.droste.{Basis, scheme, Algebra, CVCoalgebra}
import higherkindness.droste.data._
import higherkindness.droste.syntax.all._
import iota.CopK
import monocle.Prism

import org.bson._

import Pipeline._
import Projection._

object Compiler {
  def coreOpMinVersion[U: Basis[CoreOp, ?]]: U => Version =
    scheme.cata[CoreOp, U, Version](Algebra { (x: CoreOp[Version]) =>
      val order = Order[Version]
      import order._
      CopK.RemoveL[Op, CoreOpL].apply(x) match {
        case Left(a) =>
          Version.zero
        case Right(x) =>
          val fMin = x.minVersion
          x match {
            case Op.Let(vars, in) =>
              order.max(vars.toList.map(_._2).foldLeft(in)(max), fMin)
            case Op.Type(a) =>
              max(fMin, a)
            case Op.Eq(a) =>
              a.foldLeft(fMin)(max)
            case Op.Or(a) =>
              a.foldLeft(fMin)(max)
            case Op.Cond(a, b, c) =>
              List(a, b, c).foldLeft(fMin)(max)
            case Op.Ne(a) =>
              max(fMin, a)
            case Op.Gte(a) =>
              max(fMin, a)
            case Op.ObjectToArray(a) =>
              max(fMin, a)
            case Op.ArrayElemAt(a, _) =>
              max(fMin, a)
            case Op.Reduce(a, b ,c) =>
              List(a, b, c).foldLeft(fMin)(max)
            case Op.ConcatArrays(a) =>
              a.foldLeft(fMin)(max)
            case Op.Not(a) =>
              max(fMin, a)
            case Op.Exists(a) =>
              max(fMin, a)
          }
      }
    })

  def compileOp[W: Basis[CoreOp, ?], U: Basis[Core0, ?]]: W => U = {
    val core0 = Optics.core(Optics.basisPrism[Core0, U])
    import core0._
    scheme.cata[CoreOp, W, U](Algebra { (x: CoreOp[U]) =>
      CopK.RemoveL[Op, CoreOpL].apply(x) match {
        case Left(a) =>
          computeListK(a).embed
        case Right(b) => b match {
          case Op.Let(vars, in) => obj(Map("$let" -> obj(Map("vars" -> obj(vars), "in" -> in))))
          case Op.Type(a) => obj(Map("$type" -> a))
          case Op.Eq(a) => obj(Map("$eq" -> array(a)))
          case Op.Or(a) => obj(Map("$or" -> array(a)))
          case Op.Exists(a) => obj(Map("$exists" -> a))
          case Op.Cond(a, b, c) => obj(Map("$cond" -> array(List(a, b, c))))
          case Op.Ne(a) => obj(Map("$ne" -> a))
          case Op.Gte(a) => obj(Map("$gte" -> a))
          case Op.ObjectToArray(a) => obj(Map("$objectToArray" -> a))
          case Op.ArrayElemAt(a, ix) => obj(Map("$arrayElemAt" -> array(List(a, int(ix)))))
          case Op.Reduce(a, b, c) => obj(Map("$reduce" -> obj(Map(
            "input" -> a,
            "initialValue" -> b,
            "in" -> c))))
          case Op.ConcatArrays(a) => obj(Map("$concatArrays" -> array(a)))
          case Op.Not(a) => obj(Map("$not" -> a))
        }
      }
    })
  }

  type Elem = (Projection.Grouped, Int)
  private def unfoldProjectionψ(uuid: String): CVCoalgebra[CoreOp, List[Elem]] = {
    type U = Coattr[CoreOp, List[Elem]]
    type F[X] = CoattrF[CoreOp, List[Elem], X]

    val toCopK = Optics.basisPrism[F, U] composePrism Optics.coattrFPrism[CoreOp, List[Elem], U]
    val ff = Optics.coreOp(Prism.id: Prism[CoreOp[U], CoreOp[U]])
    val coreOp = Optics.coreOp(toCopK)
    import coreOp._

    CVCoalgebra {
      case List() =>
        ff.str("$$ROOT")
      case (Grouped.FieldGroup(hd :: tail), _) :: List() =>
        ff.str(tail.foldLeft("$".concat(hd)){ (acc, s) => s"$acc.$s" })
      case (hd, levelIx) :: tl =>
        val level = s"level${levelIx.toString}"
        val tailCont: U = Coattr.pure(tl)
        val undefined = str(s"$uuid$MissingSuffix")
        hd match {
          case Grouped.IndexGroup(i) =>
            ff.let(
              Map(level -> tailCont),
              cond(
                eqx(List(typ(tailCont), str("array"))),
                arrayElemAt(str("$$".concat(level)), i),
                undefined))
          case Grouped.FieldGroup(steps) =>
            val expSteps = Grouped.FieldGroup("$".concat(level) :: steps)
            val exp: U = Coattr.pure(List((expSteps, 0)))
            ff.let(
              Map(level -> tailCont),
              exp)
      }
    }
  }

  private def unfoldProjection[U: Basis[CoreOp, ?]](uuid: String): Projection => U = { (prj: Projection) =>
    val grouped = Projection.Grouped(prj).zipWithIndex
    val fn: List[Elem] => U = scheme.zoo.futu(unfoldProjectionψ(uuid))
    fn(grouped)
  }

  def compileProjections[U: Basis[Expr, ?], W: Basis[CoreOp, ?]](uuid: String): U => W =
    scheme.cata[Expr, U, W](Algebra { x =>
      CopK.RemoveL[Const[Projection, ?], ExprL].apply(x) match {
        case Left(a) =>
          computeListK(a).embed
        case Right(Const(prj)) =>
          val fn = unfoldProjection[W](uuid)
          fn(prj)
      }
    })

  def eraseCustomPipeline[F[a] <: ACopK[a], U](
      uuid: String,
      pipeline: Pipeline[U])(
      implicit
      IC: Core :<<: F,
      IO: Op :<<: F,
      U: Basis[F, U])
      : List[MongoPipeline[U]] = {
    val o = Optics.coreOp(Optics.basisPrism[F, U])
    import o._

    pipeline match {
      case Presented =>
        List(Match(obj(Map(uuid -> nex(missing[F, U](uuid))))))
      case Erase =>
        List(Match(obj(Map(uuid.concat("_erase") -> bool(false)))))
      case Project(obj) =>
        List(Project(obj))
      case Match(obj) =>
        List(Match(obj))
      case Unwind(a, i) =>
        List(Unwind(a, i))
    }
  }

  def pipelineObjects[F[a] <: ACopK[a], U](pipe: MongoPipeline[U])(implicit I: Core :<<: F, U: Basis[F, U]): U = {
    val o = Optics.core(Optics.basisPrism[F, U])
    pipe match {
      case Match(mp) =>
        o.obj(Map("$match" -> mp))
      case Project(mp) =>
        o.obj(Map("$project" -> o.obj(mp)))
      case Unwind(path, arrayIndex) =>
        o.obj(Map("$unwind" -> o.obj(Map(
          "path" -> o.str("$".concat(path)),
          "includeArrayIndex" -> o.str(arrayIndex),
          "preserveNullAndEmptyArrays" -> o.bool(false)))))
    }
  }

  def letOpt[W: Basis[CoreOp, ?], U: Basis[CoreOp, ?]]: W => U = {
    val o = Optics.coreOp(Optics.basisPrism[CoreOp, U])
    val f = Optics.coreOp(Prism.id[CoreOp[U]])
    import o._
    scheme.cata[CoreOp, W, U](Algebra {
      case l @ f.let(vars, in) =>
        if (vars.size =!= 1) let(vars, in)
        else in match {
          case str(k) =>
            vars.get(k.stripPrefix("$$")) match {
              case Some(str(v)) if v.stripPrefix("$") =!= v =>
                str(v.concat(v))
              case _ =>
                let(vars, in)
            }
          case _ =>
            let(vars, in)
        }
      case x => x.embed
    })
  }

  def orOpt[W: Basis[CoreOp, ?], U: Basis[CoreOp, ?]]: W => U = {
    val o = Optics.coreOp(Optics.basisPrism[CoreOp, U])
    val f = Optics.coreOp(Prism.id[CoreOp[U]])
    import o._
    scheme.cata[CoreOp, W, U](Algebra {
      case f.or(lst) => lst match {
        case List() => bool(false)
        case List(a) => a
        case _ => or(lst flatMap {
          case or(as) => as
          case x => List(x)
        })
      }
      case x => x.embed
    })
  }
  def mapProjection[F[a] <: ACopK[a], W, U](fn: Projection => Projection)(
      implicit
      F: Functor[F],
      I: Const[Projection, ?] :<<: F,
      Q: Basis[F, W],
      P: Basis[F, U])
      : W => U = {
    val o = Optics.projection(Optics.basisPrism[F, U] composePrism Optics.copkPrism[Const[Projection, ?], F, U])
    val f = Optics.projection(Optics.copkPrism[Const[Projection, ?], F, U])
    scheme.cata[F, W, U](Algebra {
      case f.projection(prj) =>
        o.projection(fn(prj))
      case x =>
        x.embed
    })
  }

  def coreToBson[U: Basis[Core0, ?]]: U => BsonValue = {
    import scala.collection.JavaConverters._
    scheme.cata[Core0, U, BsonValue](Algebra { x =>
      CopK.RemoveL[Core, CoreL].apply(x) match {
        case Right(a) => a match {
          case Core.Null() =>
            new BsonNull()
          case Core.Int(i) =>
            new BsonInt32(i)
          case Core.Long(l) =>
            new BsonInt64(l)
          case Core.Double(d) =>
            new BsonDouble(d)
          case Core.String(s) =>
            new BsonString(s)
          case Core.DateTime(d) =>
            new BsonDateTime(d.toEpochSecond)
          case Core.Bool(b) =>
            new BsonBoolean(b)
          case Core.Array(as) =>
            new BsonArray(as.asJava)
          case Core.Object(mp) =>
            val elems = mp.toList.map {
              case (k, v) => new BsonElement(k, v)
            }
            new BsonDocument(elems.asJava)
        }
        case Left(_) => throw new Throwable("impossible")
    }})
  }

  def compile[F[_]: MonoidK: Monad, U: Basis[Expr, ?]](
      version: Version,
      uuid: String,
      pipes: List[Pipeline[U]]): F[List[BsonDocument]] =
    pipes.flatMap(eraseCustomPipeline[Expr, U](uuid, _)).traverse { (pipe: MongoPipeline[U]) =>
      if (version < pipe.minVersion) MonoidK[F].empty
      else {
        val exprs = pipelineObjects[Expr, U](pipe)
        val coreop: Fix[CoreOp] = compileProjections[U, Fix[CoreOp]](uuid).apply(exprs)
        val optimized = (letOpt[Fix[CoreOp], Fix[CoreOp]] andThen orOpt[Fix[CoreOp], Fix[CoreOp]])(coreop)
        val opVersion = coreOpMinVersion[Fix[CoreOp]].apply(optimized)
        if (opVersion > version) MonoidK[F].empty
        else {
          val core: Fix[Core0] = compileOp[Fix[CoreOp], Fix[Core0]].apply(optimized)
          val bsonValue = coreToBson[Fix[Core0]].apply(core)
          onlyDocuments[F](bsonValue)
        }
      }
    }

  def onlyDocuments[F[_]: MonoidK: Applicative](inp: BsonValue): F[BsonDocument] = inp match {
    case x: BsonDocument => x.pure[F]
    case _ => MonoidK[F].empty
  }
}
