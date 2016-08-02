package org.opencypher.spark

import org.apache.spark.sql._

trait PropertyGraph {

  /**
    * Each row contains a node.
    * Each node has three columns: id, labels, properties.
    * The id column is a long containing the node id.
    * The labels column is a list/set/??? containing the node's labels.
    * The properties column is a map from String to Any, containing the node's properties.
    */
  def nodes: Dataset[CypherNode]

  /**
    * Each row contains a relationship.
    * Each relationship has five columns: id, start, end, type, properties.
    * The id column is a long containing the relationship id.
    * The start column is a long containing the id of relationship's start node.
    * The end column is a long containing the id of relationship's end node.
    * The type column is a string containing the relationship type.
    * The properties column is a map from String to Any, containing the relationships's properties.
    */
  def relationships: Dataset[CypherRelationship]

  def cypher(query: String): CypherResult
}



