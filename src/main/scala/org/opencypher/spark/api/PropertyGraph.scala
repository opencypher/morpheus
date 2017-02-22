package org.opencypher.spark.api

import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.SupportedQuery
import org.opencypher.spark.prototype.Expr
import org.opencypher.spark.prototype.ir.QueryModel

trait PropertyGraph {
  def cypher(query: SupportedQuery): CypherResultContainer

  def cypherNew(ir: QueryModel[Expr], params: Map[String, CypherValue]): CypherResultContainer
}



