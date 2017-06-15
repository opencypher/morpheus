package org.opencypher.spark.impl.instances

import org.opencypher.spark.api.value.{CypherInteger, CypherString}
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.impl.syntax.cypher._
import org.opencypher.spark.{GraphMatchingTestSupport, TestSession, TestSuiteImpl}

class ExpressionAcceptanceTest extends TestSuiteImpl with GraphMatchingTestSupport with TestSession.Fixture {

  test("property expression") {
    val theGraph = """(:Person {name: "Mats"})-->(:Person {name: "Martin"})"""

    val graph = theGraph.toGraph

    val result = graph.cypher("MATCH (p:Person) RETURN p.name")

    result.records.toMaps should equal(Set(
      Map("p.name" -> CypherString("Mats")),
      Map("p.name" -> CypherString("Martin"))
    ))
    result.graph shouldMatch theGraph
  }

  test("property expression with relationship") {
    val theGraph = """(:Person {name: "Mats"})-[:KNOWS {since: 2017l}]->(:Person {name: "Martin"})"""

    val graph = theGraph.toGraph

    val result = graph.cypher("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since")

    result.records.toMaps should equal(Set(
      Map("r.since" -> CypherInteger(2017))
    ))
    result.graph shouldMatch theGraph
  }

}

