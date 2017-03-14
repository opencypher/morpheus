package org.opencypher.spark.prototype

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.CypherRecord
import org.opencypher.spark.benchmark.RunBenchmark

class PrototypeTest extends StdTestSuite {

  val engine = new Prototype {
    override def graph = RunBenchmark.createStdPropertyGraphFromNeo(-1)
  }

  ignore("run cypher query") {
    val query = "MATCH (a:Administrator)-->(g:Group) WHERE g.name = 'Group-1' RETURN a.name"

    val result = engine.cypher(query)

    val start = System.currentTimeMillis()
    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("a_name" -> "Administrator-583"),
      CypherRecord("a_name" -> "Administrator-1")
    ))
    println(s"Time: ${System.currentTimeMillis() - start}")
  }
}
