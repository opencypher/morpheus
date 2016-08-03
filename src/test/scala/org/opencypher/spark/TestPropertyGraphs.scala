package org.opencypher.spark

import org.apache.spark.sql.{SparkSession, SQLContext, Dataset}
import org.opencypher.spark.impl.{StdPropertyGraphFactory, StdPropertyGraph}

object TestPropertyGraphs {

  val session: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
  val sc = session.sqlContext

  def graph1: PropertyGraph = {
    val factory = new StdPropertyGraphFactory(sc)
    val n0 = factory.addNode(Map("prop" -> CypherString("value")))
    val n1 = factory.addLabeledNode("Label")()
    val n2 = factory.addLabeledNode("Label1", "Label2")()
    val n3 = factory.addNode(Map("prop" -> CypherString("foo")))
    factory.addRelationship(n0, n1, "KNOWS")
    factory.result
  }
}


