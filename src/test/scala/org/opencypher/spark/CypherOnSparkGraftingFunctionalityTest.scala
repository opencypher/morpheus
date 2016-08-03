package org.opencypher.spark

import org.apache.spark.sql.{Encoders, Dataset}
import org.opencypher.spark.impl.StdPropertyGraph
import org.scalatest.FunSuite

class CypherOnSparkGraftingFunctionalityTest extends FunSuite {

  import StdPropertyGraph.SupportedQueries
  import TestPropertyGraphs.session.implicits._
  import CypherValue.implicits._


  test("all node scan") {
    val pg: PropertyGraph = TestPropertyGraphs.graph1

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScan)
    val result = cypher.toDS { map =>
      map.toString()
    }.show()
  }

  test("all node scan on node-only graph that uses all kinds of properties") {
    val pg: PropertyGraph = TestPropertyGraphs.graph2

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScan)
    val result = cypher.toDS { map =>
      map.toString()
    }.show()
  }

  test("get all node ids") {
    val pg: PropertyGraph = TestPropertyGraphs.graph1

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodeIds)
    val result: Dataset[String] = cypher.toDS { map =>
      map.toString()
    }

    result.show(false)
  }

  test("get all node ids sorted desc") {
    val pg: PropertyGraph = TestPropertyGraphs.graph1

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodeIdsSortedDesc)
    val result: Dataset[String] = cypher.toDS { map =>
      map.toString()
    }

    result.show(false)
  }

  test("get all nodes and project two properties into multiple columns using toDS()") {
    val pg: PropertyGraph = TestPropertyGraphs.graph3

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScanProjectAgeName)
    val result = cypher.toDS { record =>
      (record("name"), record("age"))
    }

    result.show(false)
  }

  test("get all nodes and project two properties into multiple columns using toDF()") {
    val pg: PropertyGraph = TestPropertyGraphs.graph3

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScanProjectAgeName)
    val result = cypher.toDF

    result.show(false)
  }

  test("get all rels of type T") {
    val pg: PropertyGraph = TestPropertyGraphs.graph1

    val cypher: CypherResult = pg.cypher(SupportedQueries.getAllRelationshipsOfTypeT)

    cypher.show()
  }
  test("get all rels of type T from nodes of label A") {
    val pg: PropertyGraph = TestPropertyGraphs.graph1

    val cypher: CypherResult = pg.cypher(SupportedQueries.getAllRelationshipsOfTypeTOfLabelA)

    cypher.show()
  }
}
