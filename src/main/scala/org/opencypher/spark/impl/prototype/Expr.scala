package org.opencypher.spark.impl.prototype

sealed trait Expr {
  def usedFields: Set[Field] = Set.empty
  def usedLabels: Set[LabelRef] = Set.empty
  def usedRelTypes: Set[RelTypeRef] = Set.empty
  def usedPropertyKeys: Set[PropertyKeyRef] = Set.empty
}

final case class Var(name: String) extends Expr

final case class Connected(source: Field, rel: Field, target: Field) extends Expr {
  override def usedFields = Set(source, rel, target)
}

// TODO: NonEmptySet(?)
object Ands {
  def apply(exprs: Expr*): Ands = Ands(exprs.toSet)
}

final case class Ands(exprs: Set[Expr]) extends Expr
final case class HasLabel(node: Expr, label: LabelRef) extends Expr
final case class HasType(rel: Expr, relType: RelTypeRef) extends Expr
final case class Equals(lhs: Expr, rhs: Expr) extends Expr

case class Property(m: Expr, key: PropertyKeyRef) extends Expr

sealed trait Literal extends Expr
final case class IntegerLit(v: Long) extends Literal
final case class StringLit(v: String) extends Literal

