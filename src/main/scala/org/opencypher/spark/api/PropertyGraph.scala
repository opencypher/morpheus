package org.opencypher.spark.api

import org.opencypher.spark.impl.SupportedQuery

trait PropertyGraph {
  def cypher(query: SupportedQuery): CypherResultContainer
}



