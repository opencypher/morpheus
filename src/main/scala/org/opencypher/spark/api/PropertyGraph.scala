package org.opencypher.spark.api

import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.SupportedQuery
import org.opencypher.spark.impl.prototype.{Expr, QueryRepresentation}

trait PropertyGraph {
  def cypher(query: SupportedQuery): CypherResultContainer

  def cypherNew(ir: QueryRepresentation[Expr], params: Map[String, CypherValue]): CypherResultContainer
}



