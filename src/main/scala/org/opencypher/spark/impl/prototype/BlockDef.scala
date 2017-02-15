package org.opencypher.spark.impl.prototype

trait BlockDef {
  def blockType: BlockType
  def isLeaf = dependencies.isEmpty
  def dependencies: Seq[BlockRef]

  def inputs: Seq[Field]
  def outputs: Seq[Field]
}

sealed trait BlockType {
  def name: String

  override def toString: String = name
}

sealed trait MatchBlockType extends BlockType
case object StandardMatchBlockType extends MatchBlockType { override def name = "match" }
case object OptionalMatchBlockType extends MatchBlockType { override def name = "optional-match" }

final case class Field(name: String) extends AnyVal
final case class BlockRef(name: String) extends AnyVal

