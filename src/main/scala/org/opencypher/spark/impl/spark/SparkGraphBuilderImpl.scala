package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.spark._

class SparkGraphBuilderImpl(graphSpace: SparkGraphSpace) extends SparkGraphBuilder {
  override def withNodesDF(df: DataFrame, id: String): SparkNodesMapper = ???
  override def withRelationshipsDF(df: DataFrame, ids: (String, String, String)): SparkRelationshipsMapper = ???

  override def graph: SparkCypherGraph = SparkCypherGraph.empty(graphSpace)
}
