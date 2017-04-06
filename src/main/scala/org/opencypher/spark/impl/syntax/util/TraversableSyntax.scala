package org.opencypher.spark.impl.syntax.util

import cats.Monoid
import cats.syntax.monoid._

import scala.language.implicitConversions

trait TraversableSyntax {
  implicit def traversableSyntax[E](elts: Traversable[(E)]): TraversableOps[E] =
    new TraversableOps[E](elts)
}

final class TraversableOps[E](val elts: Traversable[E]) {
  def groups[K, V](implicit ev: E =:= (K, V), monoid: Monoid[V]): Map[K, V] =
    groupsFrom(ev)

  def groupsFrom[K, V](f: E => (K, V))(implicit monoid: Monoid[V]): Map[K, V] = {
    elts.foldLeft(Map.empty[K, V]) {
      case (m, elt) =>
        val (k, v) = f(elt)
        m.get(k) match {
          case Some(values) => m.updated(k, values |+| v)
          case None => m.updated(k, v)
        }
    }
  }
}
