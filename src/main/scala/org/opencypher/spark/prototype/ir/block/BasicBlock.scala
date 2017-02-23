package org.opencypher.spark.prototype.ir.block

trait BasicBlock[B <: Binds[E], E, T <: BlockType] extends Block[E] {
  override def blockType: T
  override def binds: B
}
