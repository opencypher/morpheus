package org.opencypher.spark.impl.prototype

trait BlockDef {
  def blockType: BlockType
  def isLeaf: Boolean = signature.dependencies.isEmpty
  def signature: BlockSignature
}

final case class BlockSignature(dependencies: Set[BlockRef], inputs: Set[Field], outputs: Set[Field])

sealed trait BlockType {
  def name: String

  override def toString: String = name
}

sealed trait MatchBlockType extends BlockType
case object StandardMatchBlockType extends MatchBlockType { override def name = "match" }
case object OptionalMatchBlockType extends MatchBlockType { override def name = "optional-match" }

case object ReturnBlockType extends BlockType { override def name: String = "return" }

final case class Field(name: String) extends AnyVal
final case class BlockRef(name: String) extends AnyVal

