package quasar.physical.mongo

import slamdata.Predef._

import scalaz.{Applicative, Traverse}

trait Core[T[_[_]], A] extends Product with Serializable
trait Op[T[_[_]], A] extends Product with Serializable
trait Projection[T[_[_]], A] extends Product with Serializable

object Core {
  final case class Array[T[_[_]], A](arr: List[A]) extends Core[T, A]
  final case class Object[T[_[_]], A](obj: Map[String, A]) extends Core[T, A]
  final case class Bool[T[_[_]], A](bool: Boolean) extends Core[T, A]
  final case class Integer[T[_[_]], A](int: Int) extends Core[T, A]
  final case class Text[T[_[_]], A](text: String) extends Core[T, A]
  final case object Null[T[_[_]], A] extends Core[T, A]

  implicit def traverse[T[_[_]]]: Traverse[Core[T, ?]] = new Traverse[Core[T, ?]] {
    def traverseImpl[G[_]: Applicative, A, B](core: Core[T, A])(f: A => G[B])
        : G[Core[T, B]] = core match {

      case Null => kik //Null.point[G]
    }
  }
}

object Op {
}

object Projection {
}
