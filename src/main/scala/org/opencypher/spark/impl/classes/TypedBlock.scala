package org.opencypher.spark.impl.classes

import org.opencypher.spark.api.ir.Field

trait TypedBlock[Block] {

  type BlockExpr

  def outputs(block: Block): Set[Field]
}
