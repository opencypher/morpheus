package org.opencypher.spark.impl.classy

import org.opencypher.spark.api.ir.Field

trait TypedBlock[Block] {

  type BlockExpr

  def outputs(block: Block): Set[Field]
}
