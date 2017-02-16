package org.opencypher.spark.impl.prototype

import scala.language.implicitConversions

trait BlockDef {
  def blockType: BlockType
  def isLeaf: Boolean = after.isEmpty

  def after: Set[BlockRef]
  def over: BlockSignature
}

case object BlockSignature {
  implicit def signature(pairs: (Set[Field], Set[Field])): BlockSignature = BlockSignature(pairs._1, pairs._2)
}

final case class BlockSignature(inputs: Set[Field], outputs: Set[Field])

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

