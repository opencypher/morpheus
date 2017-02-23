package org.opencypher.spark.prototype.ir.block

sealed trait BlockType {
  def name: String

  override def toString: String = name
}

sealed trait MatchBlockType extends BlockType
case object StandardMatchBlockType extends MatchBlockType { override def name = "match" }
case object OptionalMatchBlockType extends MatchBlockType { override def name = "optional-match" }

case object ProjectBlockType extends BlockType { override def name = "project"}
case object SelectBlockType extends BlockType { override def name = "select" }
