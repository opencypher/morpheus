package org.opencypher.spark.impl.prototype

import org.opencypher.spark.impl.SupportedQuery

class SupportedQueryPlanner extends SparkCypherPlanner {
  override def plan(sparkQueryGraph: QueryRepresentation[Expr]): SupportedQuery = {
    val blocks = sparkQueryGraph.root.blocks

    blocks.blocks(blocks.solve) match {
      case _: MatchBlock[_] => ???
    }
  }
}
