package org.opencypher.spark.legacy.api

import org.opencypher.spark.prototype.api.value.CypherValue
import org.opencypher.spark.legacy.impl.SupportedQuery
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.impl.logical.LogicalOperator

trait PropertyGraph {
  def cypher(query: SupportedQuery): CypherResultContainer

  def cypherNew(plan: LogicalOperator, globals: GlobalsRegistry, params: Map[String, CypherValue]): CypherResultContainer
}



