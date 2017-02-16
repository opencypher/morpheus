package org.opencypher.spark.impl.prototype

trait BasicBlockDef extends BlockDef {
  def given: Set[AnyEntity]
  def predicates: Set[Predicate]
}

case class MatchBlock(signature: BlockSignature, given: Set[AnyEntity], predicates: Set[Predicate]) extends BasicBlockDef {
  override val blockType = StandardMatchBlockType
}

sealed trait AnyEntity
final case class AnyNode(field: Field) extends AnyEntity
final case class AnyRelationship(field: Field) extends AnyEntity

final case class Predicate(expr: Expr)
