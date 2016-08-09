package org.opencypher.spark.api

trait PropertyGraph {
  def cypher(query: String): CypherResultContainer
}



