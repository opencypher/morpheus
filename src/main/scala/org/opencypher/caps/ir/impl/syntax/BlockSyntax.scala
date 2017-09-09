package org.opencypher.caps.ir.impl.syntax

import org.opencypher.caps.common.classes.TypedBlock
import org.opencypher.caps.ir.api.IRField
import org.opencypher.caps.ir.api.block.Block

import scala.language.implicitConversions

trait BlockSyntax {
  implicit def typedBlockOps[B <: Block[_], E](block: B)(implicit instance: TypedBlock[B] { type BlockExpr = E })
  : TypedBlockOps[B, E] =
    new TypedBlockOps[B, E](block)
}

final class TypedBlockOps[B <: Block[_], E](block: B)(implicit instance: TypedBlock[B] { type BlockExpr = E }) {
  def outputs: Set[IRField] = instance.outputs(block)
}
