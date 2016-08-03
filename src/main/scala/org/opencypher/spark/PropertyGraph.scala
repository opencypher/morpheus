package org.opencypher.spark

import org.apache.spark.sql._

trait PropertyGraph {

  def nodes: Dataset[CypherNode]

  def relationships: Dataset[CypherRelationship]

  def cypher(query: String): CypherResult
}



