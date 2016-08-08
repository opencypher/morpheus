package org.opencypher.spark

import org.apache.spark.sql._

trait PropertyGraph {
//
//  def nodes: CypherFrame[CypherNode]
//
//  def relationships: CypherFrame[CypherRelationship]

  def cypher(query: String): CypherResult[Row]
}



