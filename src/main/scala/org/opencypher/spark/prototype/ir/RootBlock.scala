package org.opencypher.spark.prototype.ir

import org.opencypher.spark.prototype.{Param, Var}

import scala.language.implicitConversions

trait RootBlock[E] {
  def outputs: Set[Field]

  def params: Set[Param]
  def variables: Set[Var]
  def tokens: TokenRegistry
  def blocks: Blocks[E]

  def solve: BlockDef[E] = blocks(blocks.solve)
}

trait Blocks[E] extends (BlockRef => BlockDef[E]) {
  def blocks: Map[BlockRef, BlockDef[E]]
  def solve: BlockRef
}

trait BlockDef[E] {
  def blockType: BlockType
  def isLeaf: Boolean = after.isEmpty

  def after: Set[BlockRef]
  def over: BlockSignature
}

final case class BlockRef(name: String) extends AnyVal
