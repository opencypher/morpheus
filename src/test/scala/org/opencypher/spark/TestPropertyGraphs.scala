package org.opencypher.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.impl.StdPropertyGraphFactory

object TestPropertyGraphs {

  implicit val session: SparkSession = SparkSession.builder().master("local[4]").getOrCreate()

  import EntityData._
  import CypherValue.implicits._


  def graph1: PropertyGraph = {
    val factory = new StdPropertyGraphFactory
    val n1 = factory.add(newNode.withProperties("prop" -> CypherString("value")))
    val n2 = factory.add(newLabeledNode("B"))
    val n3 = factory.add(newLabeledNode("A", "B"))
    val n4 = factory.add(newLabeledNode("A").withProperties("prop" -> "foo"))
    factory.add(newRelationship(n1 -> "KNOWS" -> n2))
    factory.add(newRelationship(n4 -> "T" -> n2))
    factory.graph
  }

  def graph2: PropertyGraph = {

    import CypherValue.implicits._

    val factory = new StdPropertyGraphFactory
    factory.add(newNode.withProperties("prop" -> "value"))
    factory.add(newNode.withProperties("prop" -> true))
    factory.add(newNode.withProperties("prop" -> 42))
    factory.add(newNode.withProperties("prop" -> 23.1))
    factory.add(newNode.withProperties("prop" -> Seq(CypherString("Hallo"), CypherBoolean(true))))
    factory.add(newNode.withProperties("prop" -> Seq(Map[String, CypherValue]("a" -> "Hallo", "b" -> true))))
    factory.graph
  }


  def graph3: PropertyGraph = {

    val factory = new StdPropertyGraphFactory
    factory.add(newLabeledNode("B").withProperties("name" -> "Sasha", "age" -> 4))
    factory.add(newLabeledNode("B").withProperties("name" -> "Sasha", "age" -> 16))
    factory.add(newLabeledNode("B").withProperties("name" -> "Ava", "age" -> 2))
    factory.add(newLabeledNode("A").withProperties("name" -> "Mats", "age" -> 28))
    factory.add(newLabeledNode("A").withProperties("name" -> "Stefan", "age" -> 37))
    factory.add(newLabeledNode("A").withProperties("name" -> "Stefan", "age" -> 58))
    factory.add(newLabeledNode("B").withProperties("name" -> "Stefan", "age" -> 4))
    factory.add(newLabeledNode("A"))
    factory.add(newLabeledNode("B"))
    factory.graph
  }

  def graph4: PropertyGraph = {

    val factory = new StdPropertyGraphFactory
    val n1 = factory.add(newLabeledNode("A"))
    val n2 = factory.add(newLabeledNode("A"))
    val n3 = factory.add(newLabeledNode("A"))
    val n4 = factory.add(newLabeledNode("A"))
    val n5 = factory.add(newLabeledNode("A"))
    val n6 = factory.add(newLabeledNode("B"))
    val n7 = factory.add(newLabeledNode("A"))
    val n8 = factory.add(newLabeledNode("A"))
    val n9 = factory.add(newLabeledNode("A"))

    factory.add(newRelationship(n1 -> "T" -> n2))
    factory.add(newRelationship(n2 -> "T" -> n3))
    factory.add(newRelationship(n2 -> "T" -> n6))
    factory.add(newRelationship(n4 -> "T" -> n8))
    factory.add(newRelationship(n5 -> "T" -> n8))

    factory.graph
  }
}


