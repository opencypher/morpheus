package org.opencypher.spark

import org.apache.spark.sql.{SparkSession, SQLContext, Dataset}
import org.opencypher.spark.impl.{StdPropertyGraphFactory, StdPropertyGraph}

object TestPropertyGraphs {

  val session: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()

  def graph1: PropertyGraph = {
    val factory = new StdPropertyGraphFactory(session)
    val n0 = factory.addNode(Map("prop" -> CypherString("value")))
    val n1 = factory.addLabeledNode("Label")()
    val n2 = factory.addLabeledNode("Label1", "Label2")()
    val n3 = factory.addNode(Map("prop" -> CypherString("foo")))
    factory.addRelationship(n0, n1, "KNOWS")
    factory.result
  }

  def graph2: PropertyGraph = {
    val factory = new StdPropertyGraphFactory(session)
    factory.addNode(Map("prop" -> CypherString("value")))
    factory.addNode(Map("prop" -> CypherBoolean(true)))
    factory.addNode(Map("prop" -> CypherInteger(42)))
    factory.addNode(Map("prop" -> CypherFloat(23.1)))
    factory.addNode(Map("prop" -> CypherList.of(CypherString("Hallo"), CypherBoolean(true))))
    factory.addNode(Map("prop" -> CypherList.of(CypherMap.of("a" -> CypherString("Hallo"), "b" -> CypherBoolean(true)))))
    factory.result
  }
}


