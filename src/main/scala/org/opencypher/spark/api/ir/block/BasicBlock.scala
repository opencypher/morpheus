package org.opencypher.spark.api.ir.block

abstract class BasicBlock[B <: Binds[E], E](override val blockType: BlockType) extends Block[E] {
  override def binds: B
}
