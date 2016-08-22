package org.opencypher.spark

import org.opencypher.spark.api._
import org.opencypher.spark.impl.newvalue.{CypherMap, CypherString, CypherValue}

object TestPropertyGraphs {

  import org.opencypher.spark.impl.newvalue.EntityData.Creation._
  import org.opencypher.spark.impl.newvalue.CypherValue.Conversion._

  def createGraph1(factory: PropertyGraphFactory) = {
    val n1 = factory.add(newNode.withProperties("prop" -> CypherString("value")))
    val n2 = factory.add(newLabeledNode("B"))
    val n3 = factory.add(newLabeledNode("A", "B"))
    val n4 = factory.add(newLabeledNode("A").withProperties("prop" -> "foo"))
    factory.add(newRelationship(n1 -> "KNOWS" -> n2))
    factory.add(newRelationship(n4 -> "T" -> n2))
  }

  def createGraph2(factory: PropertyGraphFactory) = {
    factory.add(newNode.withProperties("prop" -> "value"))
    factory.add(newNode.withProperties("prop" -> true))
    factory.add(newNode.withProperties("prop" -> 42))
    factory.add(newNode.withProperties("prop" -> 23.1))
    factory.add(newNode.withProperties("prop" -> Vector[CypherValue]("Hallo", true)))
    factory.add(newNode.withProperties("prop" -> Vector(CypherMap("a" -> "Hallo", "b" -> true))))
  }

  def createGraph3(factory: PropertyGraphFactory) = {
    factory.add(newLabeledNode("B").withProperties("name" -> "Sasha", "age" -> 4))
    factory.add(newLabeledNode("B").withProperties("name" -> "Sasha", "age" -> 16))
    factory.add(newLabeledNode("B").withProperties("name" -> "Ava", "age" -> 2))
    factory.add(newLabeledNode("A").withProperties("name" -> "Mats", "age" -> 28))
    factory.add(newLabeledNode("A").withProperties("name" -> "Stefan", "age" -> 37))
    factory.add(newLabeledNode("A").withProperties("name" -> "Stefan", "age" -> 58))
    factory.add(newLabeledNode("B").withProperties("name" -> "Stefan", "age" -> 4))
    factory.add(newLabeledNode("A"))
    factory.add(newLabeledNode("B"))
  }

  def createGraph4(factory: PropertyGraphFactory) = {
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
  }
}


