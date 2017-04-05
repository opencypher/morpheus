package org.opencypher.spark_legacy.api

import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark_legacy.impl.SupportedQuery
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.impl.logical.LogicalOperator

trait PropertyGraph {
  def cypher(query: SupportedQuery): CypherResultContainer

  def cypherNew(plan: LogicalOperator, globals: GlobalsRegistry, params: Map[String, CypherValue]): CypherResultContainer
}



