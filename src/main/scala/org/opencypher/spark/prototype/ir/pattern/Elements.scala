package org.opencypher.spark.prototype.ir.pattern

trait Elements[T] {
  def elts: Set[T]
}

final case class AnyGiven[T](elts: Set[T] = Set.empty[T]) extends Elements[T]

case object AnyOf {
  def apply[T](elts: T*) = AnyGiven(elts.toSet)
}

final case class AllGiven[T](elts: Set[T] = Set.empty[T]) extends Elements[T]

case object AllOf {
  def apply[T](elts: T*) = AllGiven(elts.toSet)
}
