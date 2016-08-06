package org.opencypher.spark

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl.StdPropertyGraph
import org.scalatest.FunSuite

class GraftingCypherOnSparkFunctionalityTest extends FunSuite {

  import CypherValue.implicits._
  import StdPropertyGraph.SupportedQueries

  import TestSession._
  import session.implicits._
  import TestPropertyGraphs._

  test("all node scan") {
    val pg = factory(createGraph1(_)).graph

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScan)
    val result = cypher.toDS { map =>
      map.toString()
    }.show()
  }

  test("all node scan on node-only graph that uses all kinds of properties") {
    val pg = factory(createGraph2(_)).graph

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScan)
    val result = cypher.toDS { map =>
      map.toString()
    }.show()
  }

  test("get all node ids") {
    val pg = factory(createGraph1(_)).graph

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodeIds)
    val result: Dataset[String] = cypher.toDS { map =>
      map.toString()
    }

    result.show(false)
  }

  test("get all node ids sorted desc") {
    val pg = factory(createGraph1(_)).graph

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodeIdsSortedDesc)
    val result: Dataset[String] = cypher.toDS { map =>
      map.toString()
    }

    result.show(false)
  }

  test("get all nodes and project two properties into multiple columns using toDS()") {
    val pg = factory(createGraph3(_)).graph

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScanProjectAgeName)
    val result = cypher.toDS { record =>
      (record("name"), record("age"))
    }

    result.show(false)
  }

  test("get all nodes and project two properties into multiple columns using toDF()") {
    val pg = factory(createGraph3(_)).graph

    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScanProjectAgeName)
    val result = cypher.toDF

    result.show(false)
  }

  test("get all rels of type T") {
    val pg = factory(createGraph1(_)).graph

    val cypher: CypherResult = pg.cypher(SupportedQueries.getAllRelationshipsOfTypeT)

    cypher.show()
  }

  test("get all rels of type T from nodes of label A") {
    val pg = factory(createGraph1(_)).graph

    val cypher: CypherResult = pg.cypher(SupportedQueries.getAllRelationshipsOfTypeTOfLabelA)

    cypher.show()
  }

  test("simple union all") {
    val pg = factory(createGraph3(_)).graph

    val result: CypherResult = pg.cypher(SupportedQueries.simpleUnionAll)

    result.show()
  }

  test("simple union distinct") {
    val pg = factory(createGraph3(_)).graph

    val result: CypherResult = pg.cypher(SupportedQueries.simpleUnionDistinct)

    result.show()
  }

  test("optional match") {
    val pg = factory(createGraph1(_)).graph

    val result: CypherResult = pg.cypher(SupportedQueries.optionalMatch)

    result.show()
  }

  test("unwind") {
    val pg = factory(createGraph1(_)).graph

    val result: CypherResult = pg.cypher(SupportedQueries.unwind)

    result.show()
  }

  test("match aggregate unwind") {
    val pg = factory(createGraph3(_)).graph

    val result: CypherResult = pg.cypher(SupportedQueries.matchAggregateAndUnwind)

    result.show()
  }

  test("bound variable length") {
    val pg = factory(createGraph4(_)).graph

    val result: CypherResult = pg.cypher(SupportedQueries.boundVarLength)

    result.show()
  }
}
