package org.opencypher.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.impl.StdPropertyGraphFactory

object TestPropertyGraphs {

  implicit val session: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()

  def graph1: PropertyGraph = {
    val factory = new StdPropertyGraphFactory
    val n0 = factory.addNode(Map("prop" -> CypherString("value")))
    val n1 = factory.addLabeledNode("B")()
    val n2 = factory.addLabeledNode("A", "B")()
    val n3 = factory.addLabeledNode("A")(Map("prop" -> CypherString("foo")))
    factory.addRelationship(n0, n1, "KNOWS")
    factory.addRelationship(n3, n1, "T")
    factory.result
  }

  def graph2: PropertyGraph = {

    import CypherValue.implicits._

    val factory = new StdPropertyGraphFactory
    factory.addNode(Map("prop" -> "value"))
    factory.addNode(Map("prop" -> true))
    factory.addNode(Map("prop" -> 42))
    factory.addNode(Map("prop" -> 23.1))
    factory.addNode(Map("prop" -> Seq(CypherString("Hallo"), CypherBoolean(true))))
    factory.addNode(Map("prop" -> Seq(Map[String, CypherValue]("a" -> "Hallo", "b" -> true))))
    factory.result
  }


  def graph3: PropertyGraph = {

    import CypherValue.implicits._

    val factory = new StdPropertyGraphFactory
    factory.addNode(Map("name" -> "Sasha", "age" -> 4))
    factory.addNode(Map("name" -> "Ava", "age" -> 2))
    factory.addNode(Map("name" -> "Mats", "age" -> 28))
    factory.addNode(Map("name" -> "Stefan", "age" -> 37))
    factory.result
  }
}


