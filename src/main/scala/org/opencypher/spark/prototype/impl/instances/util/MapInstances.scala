package org.opencypher.spark.prototype.impl.instances.util

import cats.Monoid
import org.opencypher.spark.prototype.impl.syntax.util.traversable._

trait MapInstances {
  implicit def mapValueMonoid[K, V](implicit monoid: Monoid[V]) = new Monoid[Map[K, V]] {
    override def empty: Map[K, V] = Map.empty[K, V]
    override def combine(x: Map[K, V], y: Map[K, V])= (x ++ y).toTraversable.groups[K, V]
  }
}
