package org.opencypher.spark.prototype

import org.opencypher.spark.impl.SupportedQuery
import org.opencypher.spark.prototype.ir.QueryModel
import org.opencypher.spark.prototype.ir.impl.blocks.MatchBlock

class SupportedQueryPlanner extends SparkCypherPlanner {
  override def plan(sparkQueryGraph: QueryModel[Expr]): SupportedQuery = {
    val blocks = sparkQueryGraph.root.blocks

    blocks.blocks(blocks.solve) match {
      case _: MatchBlock[_] => ???
    }
  }
}
