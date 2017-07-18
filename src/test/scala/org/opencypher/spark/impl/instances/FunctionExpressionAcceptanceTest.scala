package org.opencypher.spark.impl.instances

import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.value.CypherMap

import scala.collection.immutable.Bag

class FunctionExpressionAcceptanceTest extends SparkCypherTestSuite {

  test("id for node") {
    val given = TestGraph("(),()")

    val result = given.cypher("MATCH (n) RETURN id(n)")

    result.records.toMaps should equal(Bag(
      CypherMap("id(n)" -> 0),
      CypherMap("id(n)" -> 1))
    )

    result.graph shouldMatch given.graph
  }

  test("id for rel") {
    val given = TestGraph("()-->()-->()")

    val result = given.cypher("MATCH ()-[e]->() RETURN id(e)")

    result.records.toMaps should equal(Bag(
      CypherMap("id(e)" -> 0),
      CypherMap("id(e)" -> 1))
    )

    result.graph shouldMatch given.graph
  }
}
