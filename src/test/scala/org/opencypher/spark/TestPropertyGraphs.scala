package org.opencypher.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.impl.StdPropertyGraphFactory

object TestPropertyGraphs {

  implicit val session: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()

  def graph1: PropertyGraph = {
    val factory = new StdPropertyGraphFactory
    val n1 = factory.addNode(Map("prop" -> CypherString("value")))
    val n2 = factory.addLabeledNode("B")()
    val n3 = factory.addLabeledNode("A", "B")()
    val n4 = factory.addLabeledNode("A")(Map("prop" -> CypherString("foo")))
    factory.addRelationship(n1, n2, "KNOWS")
    factory.addRelationship(n4, n2, "T")
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
    factory.addLabeledNode("B")(Map("name" -> "Sasha", "age" -> 4))
    factory.addLabeledNode("B")(Map("name" -> "Sasha", "age" -> 16))
    factory.addLabeledNode("B")(Map("name" -> "Ava", "age" -> 2))
    factory.addLabeledNode("A")(Map("name" -> "Mats", "age" -> 28))
    factory.addLabeledNode("A")(Map("name" -> "Stefan", "age" -> 37))
    factory.addLabeledNode("A")(Map("name" -> "Stefan", "age" -> 58))
    factory.addLabeledNode("B")(Map("name" -> "Stefan", "age" -> 4))
    factory.addLabeledNode("A")()
    factory.addLabeledNode("B")()
    factory.result
  }

  def graph4: PropertyGraph = {

    import CypherValue.implicits._

    val factory = new StdPropertyGraphFactory
    val n1 = factory.addLabeledNode("A")()
    val n2 = factory.addLabeledNode("A")()
    val n3 = factory.addLabeledNode("A")()
    val n4 = factory.addLabeledNode("A")()
    val n5 = factory.addLabeledNode("A")()
    val n6 = factory.addLabeledNode("B")()
    val n7 = factory.addLabeledNode("B")()
    val n8 = factory.addLabeledNode("B")()
    val n9 = factory.addLabeledNode("B")()

    factory.addRelationship(n1, n2, "T")
    factory.addRelationship(n2, n3, "T")
    factory.addRelationship(n2, n6, "T")
    factory.addRelationship(n4, n8, "T")
    factory.addRelationship(n5, n8, "T")

    factory.result
  }
}


