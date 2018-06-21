package org.opencypher.spark.api.io.neo4j

import java.net.URI

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTInteger, CTString}
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.spark.api.CAPSSession

class Neo4jReadOnlySourceUnitTest extends BaseTestSuite {

  private val schema = Schema.empty
    .withNodePropertyKeys("A")("foo" -> CTInteger, "bar" -> CTString.nullable)
  implicit val session: CAPSSession = mock[CAPSSession]

  it("constructs flat Cypher queries from schema") {
    val entireGraph = "allOfIt"
    val pgds = Neo4jReadOnlySource(Neo4jConfig(URI.create("test://foo")), GraphName(entireGraph))

    pgds.flatNodeQuery(GraphName(entireGraph), Set("A"), schema) should equal(
      "MATCH (n:A) RETURN id(n) AS id, n.foo, n.bar"
    )
  }
}
