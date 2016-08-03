package org.opencypher.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.impl.StdPropertyGraphFactory

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

    import CypherValue.implicits._

    val factory = new StdPropertyGraphFactory(session)
    factory.addNode(Map("prop" -> "value"))
    factory.addNode(Map("prop" -> true))
    factory.addNode(Map("prop" -> 42))
    factory.addNode(Map("prop" -> 23.1))
    factory.addNode(Map("prop" -> Seq(CypherString("Hallo"), CypherBoolean(true))))
    factory.addNode(Map("prop" -> Seq(Map[String, CypherValue]("a" -> "Hallo", "b" -> true))))
    factory.result
  }
}


