package org.opencypher.spark.impl.prototype

trait BasicBlockDef extends BlockDef {
  def given: Set[AnyEntity]
  def predicates: Set[Expr]
}

case class MatchBlock(signature: BlockSignature, given: Set[AnyEntity], predicates: Set[Expr]) extends BasicBlockDef {
  override val blockType = StandardMatchBlockType
}

sealed trait AnyEntity
final case class AnyNode(field: Field) extends AnyEntity
final case class AnyRelationship(field: Field) extends AnyEntity
