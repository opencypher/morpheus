package org.opencypher.spark.api.io.neo4j

import java.net.URI

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTFloat, CTInteger, CTString}
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.spark.api.CAPSSession

class Neo4jReadOnlySourceUnitTest extends BaseTestSuite {

  private val schema = Schema.empty
    .withNodePropertyKeys("A")("foo" -> CTInteger, "bar" -> CTString.nullable)
    .withNodePropertyKeys("B")()
    .withRelationshipPropertyKeys("TYPE")("foo" -> CTFloat.nullable)
    .withRelationshipPropertyKeys("TYPE2")()

  private val entireGraph = "allOfIt"
  private val pgds = Neo4jReadOnlySource(Neo4jConfig(URI.create("test://foo")), GraphName(entireGraph))(mock[CAPSSession])

  it("constructs flat node queries from schema") {
    pgds.flatNodeQuery(GraphName(entireGraph), Set("A"), schema) should equal(
      "MATCH (n:A) RETURN id(n) AS id, n.foo, n.bar"
    )
  }

  it("constructs flat node queries from schema without properties") {
    pgds.flatNodeQuery(GraphName(entireGraph), Set("B"), schema) should equal(
      "MATCH (n:B) RETURN id(n) AS id"
    )
  }

  it("constructs flat relationship queries from schema") {
    pgds.flatRelQuery(GraphName(entireGraph), "TYPE", schema) should equal(
      "MATCH ()-[r:TYPE]->() RETURN id(r) AS id, r.foo"
    )
  }

  it("constructs flat relationship queries from schema with no properties") {
    pgds.flatRelQuery(GraphName(entireGraph), "TYPE2", schema) should equal(
      "MATCH ()-[r:TYPE2]->() RETURN id(r) AS id"
    )
  }
}
