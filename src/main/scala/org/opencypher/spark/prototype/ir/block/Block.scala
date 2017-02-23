package org.opencypher.spark.prototype.ir.block

import org.opencypher.spark.prototype.ir.Field

import scala.language.implicitConversions

trait Block[E] {
  def blockType: BlockType
  def isLeaf: Boolean = after.isEmpty

  def after: Set[BlockRef]
  def over: BlockSig

  def binds: Binds[E]
  def where: Where[E]
}

trait Binds[E] {
  def fields: Set[Field]
}

case object Where {
  def everything[E]: Where[E] = Where[E](Set.empty)
}

final case class Where[E](predicates: Set[E])

