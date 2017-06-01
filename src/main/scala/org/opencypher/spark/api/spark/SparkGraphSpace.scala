package org.opencypher.spark.api.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.graph.GraphSpace
import org.opencypher.spark.impl.spark.{SparkGraphLoading, SparkGraphSpaceImpl, VerifiedDataFrameGraph}

trait SparkGraphSpace extends GraphSpace {
  override type Graph = SparkCypherGraph

  def session: SparkSession

  def importGraph(name: String, externalGraph: VerifiedDataFrameGraph): SparkCypherGraph = ???
}

object SparkGraphSpace extends SparkGraphLoading with Serializable {
  def createEmpty(session: SparkSession): SparkGraphSpace =
    new SparkGraphSpaceImpl(session)
}
