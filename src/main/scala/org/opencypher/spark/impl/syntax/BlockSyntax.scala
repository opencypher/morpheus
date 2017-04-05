package org.opencypher.spark.impl.syntax

import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.block.Block
import org.opencypher.spark.impl.classes.TypedBlock

import scala.language.implicitConversions

trait BlockSyntax {
  implicit def typedBlockOps[B <: Block[_], E](block: B)(implicit instance: TypedBlock[B] { type BlockExpr = E })
  : TypedBlockOps[B, E] =
    new TypedBlockOps[B, E](block)
}

final class TypedBlockOps[B <: Block[_], E](block: B)(implicit instance: TypedBlock[B] { type BlockExpr = E }) {
  def outputs: Set[Field] = instance.outputs(block)
}
