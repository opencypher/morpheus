package org.opencypher.spark.prototype

import org.opencypher.spark.impl.SupportedQuery
import org.opencypher.spark.prototype.ir.QueryDescriptor
import org.opencypher.spark.prototype.ir.block.MatchBlock

class SupportedQueryPlanner extends SparkCypherPlanner {
  override def plan(sparkQueryGraph: QueryDescriptor[Expr]): SupportedQuery = {
    val blocks = sparkQueryGraph.model

    blocks(blocks.root) match {
      case _: MatchBlock[_] => ???
    }
  }
}
