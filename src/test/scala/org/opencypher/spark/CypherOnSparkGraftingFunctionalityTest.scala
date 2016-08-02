package org.opencypher.spark

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl.StdPropertyGraph
import org.scalatest.FunSuite

class CypherOnSparkGraftingFunctionalityTest extends FunSuite {

  import StdPropertyGraph.SupportedQueries
  import TestPropertyGraphs.sc.implicits._

  test("all node scan") {
    val pg: PropertyGraph = TestPropertyGraphs.graph1

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
}
