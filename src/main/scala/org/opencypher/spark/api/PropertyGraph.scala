package org.opencypher.spark.api

import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.SupportedQuery
import org.opencypher.spark.impl.prototype.QueryRepresentation

trait PropertyGraph {
  def cypher(query: SupportedQuery): CypherResultContainer

  def cypherNew(ir: QueryRepresentation, params: Map[String, CypherValue]): CypherResultContainer
}



