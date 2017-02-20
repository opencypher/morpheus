package org.opencypher.spark.impl.prototype

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.CypherRecord
import org.opencypher.spark.benchmark.RunBenchmark

class PrototypeTest extends StdTestSuite {

  val engine = new Prototype {
    override def graph = RunBenchmark.createStdPropertyGraphFromNeo(-1)
  }

  test("run cypher query") {
    val query = "MATCH (a:Administrator)-->(g:Group) WHERE g.name = 'Group-1' RETURN a.name"

    val result = engine.cypher(query)

    result.records.collectAsScalaSet should equal(
      CypherRecord("a.name" -> "Administrator-583"),
      CypherRecord("a.name" -> "Administrator-1")
    )
  }

  test("parser error") {
    val nonQuery = "this is not a query"


  }

  test("semantic error") {
    val q = "MATCH (n) RETURN foo"
  }

}
