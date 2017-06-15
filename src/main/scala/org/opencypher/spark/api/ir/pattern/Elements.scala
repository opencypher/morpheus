package org.opencypher.spark.api.ir.pattern

trait Elements[T] {
  def elts: Set[T]
}

final case class AnyGiven[T](elts: Set[T] = Set.empty[T]) extends Elements[T] {
  def map[X](f: T => X) = AnyGiven(elts.map(f))
}

case object AnyOf {
  def apply[T](elts: T*) = AnyGiven(elts.toSet)
}

final case class AllGiven[T](elts: Set[T] = Set.empty[T]) extends Elements[T] {
  def map[X](f: T => X) = AnyGiven(elts.map(f))
}

case object AllOf {
  def apply[T](elts: T*) = AllGiven(elts.toSet)
}
