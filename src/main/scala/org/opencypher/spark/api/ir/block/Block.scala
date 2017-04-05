package org.opencypher.spark.api.ir.block

import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.pattern.AllGiven

import scala.language.implicitConversions

trait Block[E] {
  def blockType: BlockType
  def isLeaf: Boolean = after.isEmpty

  def after: Set[BlockRef]

  def outputs: Set[Field] = binds.fields

  def binds: Binds[E]
  def where: AllGiven[E]
}

final case class BlockType(name: String)

trait Binds[E] {
  def fields: Set[Field]
}

object BlockWhere {
  def unapply[E](block: Block[E]): Option[Set[E]] = Some(block.where.elts)
}

object NoWhereBlock {
  def unapply[E](block: Block[E]): Option[Block[E]] =
    if (block.where.elts.isEmpty) Some(block) else None
}


