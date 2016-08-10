package org.opencypher.spark

import org.opencypher.spark.impl.StdPropertyGraph
import org.scalatest.{FunSuite, Matchers}

class GraftingCypherOnSparkFunctionalityTest extends FunSuite with Matchers {

  import CypherValue.implicits._
  import EntityData._
  import StdPropertyGraph.SupportedQueries
  import TestSession._
  import factory._

  test("all node scan") {
    val a = add(newNode)
    val b = add(newLabeledNode("Foo", "Bar"))
    val c = add(newLabeledNode("Foo"))
    add(newRelationship(a -> "KNOWS" -> b))
    add(newRelationship(a -> "KNOWS" -> a))

    val result = factory.graph.cypher(SupportedQueries.allNodesScan)

    result.maps.collectAsScalaSet should equal(Set(
      Map[String, CypherNode]("n" -> a),
      Map[String, CypherNode]("n" -> b),
      Map[String, CypherNode]("n" -> c)
    ))
  }

//  test("all node scan on node-only graph that uses all kinds of properties") {
//    val pg = factory(createGraph2(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScan)
//    val result = cypher.map { map =>
//      map.toString()
//    }.show()
//  }
//
//  test("get all node ids") {
//    val pg = factory(createGraph1(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodeIds)
//    val result: Dataset[String] = cypher.map { map =>
//      map.toString()
//    }
//
//    result.show(false)
//  }
//
//  test("get all node ids sorted desc") {
//    val pg = factory(createGraph1(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodeIdsSortedDesc)
//    val result: Dataset[String] = cypher.map { map =>
//      map.toString()
//    }
//
//    result.show(false)
//  }
//
//  test("get all nodes and project two properties into multiple columns using toDS()") {
//    val pg = factory(createGraph3(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScanProjectAgeName)
//    val result = cypher.map { record =>
//      (record("name"), record("age"))
//    }
//
//    result.show(false)
//  }
//
  test("get all nodes and project two properties into multiple columns") {
    factory.add(newNode.withProperties("name" -> "Mats"))
    factory.add(newNode.withProperties("name" -> "Stefan", "age" -> 37))
    factory.add(newNode.withProperties("age" -> 7))
    factory.add(newNode)

    val result = factory.graph.cypher(SupportedQueries.allNodesScanProjectAgeName)

    result.maps.collectAsScalaSet should equal(Set(
      Map[String, CypherValue]("n.name" -> "Mats", "n.age" -> null),
      Map[String, CypherValue]("n.name" -> "Stefan", "n.age" -> 37),
      Map[String, CypherValue]("n.name" -> null, "n.age" -> 7),
      Map[String, CypherValue]("n.name" -> null, "n.age" -> null)
    ))
  }
//
//  test("get all rels of type T") {
//    val pg = factory(createGraph1(_)).graph
//
//    val cypher = pg.cypher(SupportedQueries.getAllRelationshipsOfTypeT)
//
//    cypher.show()
//  }
//
//  test("get all rels of type T from nodes of label A") {
//    val pg = factory(createGraph1(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.getAllRelationshipsOfTypeTOfLabelA)
//
//    cypher.show()
//  }
//
//  test("simple union all") {
//    val pg = factory(createGraph3(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.simpleUnionAll)
//
//    result.show()
//  }
//
//  test("simple union distinct") {
//    val pg = factory(createGraph3(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.simpleUnionDistinct)
//
//    result.show()
//  }
//
//  test("optional match") {
//    val pg = factory(createGraph1(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.optionalMatch)
//
//    result.show()
//  }
//
//  test("unwind") {
//    val pg = factory(createGraph1(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.unwind)
//
//    result.show()
//  }
//
//  test("match aggregate unwind") {
//    val pg = factory(createGraph3(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.matchAggregateAndUnwind)
//
//    result.show()
//  }
//
//  test("bound variable length") {
//    val pg = factory(createGraph4(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.boundVarLength)
//
//    result.show()
//  }
}
