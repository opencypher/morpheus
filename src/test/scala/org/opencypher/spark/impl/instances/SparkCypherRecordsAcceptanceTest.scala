package org.opencypher.spark.impl.instances

import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.{SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTAny, CTInteger, CTString}
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.impl.syntax.cypher._
import org.opencypher.spark.{TestSuiteImpl, TestSession}

import scala.language.reflectiveCalls

class SparkCypherRecordsAcceptanceTest extends TestSuiteImpl with TestSession.Fixture {

  test("subtraction") {
    val result = smallSpace.base.cypher("MATCH (a:User)-[r:ATTENDED]->() RETURN a.id - r.guests")

    result.records shouldHaveSize 4832 andContain 116765532
  }

  test("label scan and project") {
    // When
    val result = smallSpace.base.cypher("MATCH (a:User) RETURN a.text")

    // Then
    result.records shouldHaveSize 1806 andContain "Application Developer"
  }

  test("expand and project") {
    // When
    val result = smallSpace.base.cypher("MATCH (a:User)-[r]->(m:Meetup) RETURN a.country, m.id")

    // Then
    result.records shouldHaveSize 4832 andContain "de" -> 168960972
  }

  test("expand and project on full graph") {
    // Given
    val query = "MATCH (g:Graph)-[r:CONTAINED]->(e:Event) RETURN g.key, e.title"

    // When
    val result = fullSpace.base.cypher(query)

    // Then
    val tuple = "GraphDB-Sydney" -> "What's new and fabulous in Neo4j 2.0 with Jim Webber"

    result.records shouldHaveSize 25 andContain tuple
  }

  test("filter rels on property") {
    // Given
    val query = "MATCH (a:User)-[r:ATTENDED]->() WHERE r.response = 'no' RETURN a, r"

    // When
    val result = fullSpace.base.cypher(query)

    // Then
    // TODO: Come up with a way to construct a node in a short test tuple for containment test
    result.records shouldHaveSize 1173
  }

  test("expand and project on full graph, three properties") {
    // Given
    val query = "MATCH (t:Tweet)-[:MENTIONED]->(l:User) RETURN t.text, l.location, l.followers"

    // When
    val result = fullSpace.base.cypher(query)

    // Then
    val tuple = (
      "RT @pronovix: We created a #Drupal integration that makes it possible for non-developers to work with #Neo4j.\n\nhttps://t.co/dERL8Czwkl",
      "Szeged and Gent",
      293
    )
    result.records shouldHaveSize 815 andContain tuple
  }

  test("handle properties with same key and different type between labels") {
    // Given
    val space = initSmallSpace(Schema.empty
      .withNodeKeys("Channel")("id" -> CTString.nullable)
      .withNodeKeys("GitHub")("id" -> CTInteger.nullable), "MATCH (n) RETURN n", "RETURN 1 LIMIT 0")

    // When
    val channelResult = space.base.cypher("MATCH (c:Channel) RETURN c.id")

    // Then
    channelResult.records shouldHaveSize 78 andContain "C08JCQDTM"

    // When
    val githubResult = space.base.cypher("MATCH (g:GitHub) RETURN g.id")

    // Then
    githubResult.records shouldHaveSize 365 andContain 80841140
  }

  test("property filter in small space") {
    // When
    val result = smallSpace.base.cypher("MATCH (t:User) WHERE t.country = 'ca' RETURN t.city")

    // Then
    result.records shouldHaveSize 38 andContain "Vancouver"
  }

  test("multiple hops of expand with different reltypes") {
    // Given
    val query = "MATCH (u1:User)-[p:POSTED]->(t:Tweet)-[m:MENTIONED]->(u2:User) RETURN u1.name, u2.name, t.text"

    // When
    val records = fullSpace.base.cypher(query).records

    // Then
    val tuple = (
      "Brendan Madden",
      "Tom Sawyer Software",
      "#tsperspectives 7.6 is 15% faster with #neo4j Bolt support. https://t.co/1xPxB9slrB @TSawyerSoftware #graphviz"
    )
    records shouldHaveSize 79 andContain tuple
  }

  test("multiple hops of expand with possible reltype conflict") {
    // Given
    val query = "MATCH (u1:User)-[r1:POSTED]->(t:Tweet)-[r2]->(u2:User) RETURN u1.name, u2.name, t.text"

    // When
    val result = fullSpace.base.cypher(query)

    // Then
    val tuple = ("Brendan Madden", "Tom Sawyer Software",
      "#tsperspectives 7.6 is 15% faster with #neo4j Bolt support. https://t.co/1xPxB9slrB @TSawyerSoftware #graphviz")
    result.records shouldHaveSize 79 andContain tuple
  }

  // TODO: Reimplement union
  ignore("union rels") {
    val query1 = "MATCH (a:User)-[r:ATTENDED]->() WHERE r.response = 'no' RETURN a, r"
    val graph1 = fullSpace.base.cypher(query1)

    val query2 = "MATCH (a:User)-[r:ATTENDED]->() WHERE r.response = 'yes' RETURN a, r"
    val graph2 = fullSpace.base.cypher(query2)

    //    val result = graph1.graph.union(graph2.graph)
    //    result.records.data.count() should equal(4711)
  }

  // TODO: Reimplement intersect
  ignore("intersect rels") {
    val query1 = "MATCH (a:User)-[r:ATTENDED]->() WHERE r.response = 'no' RETURN a, r"
    val graph1 = fullSpace.base.cypher(query1)

    val query2 = "MATCH (a:User)-[r:ATTENDED]->() WHERE r.response = 'yes' RETURN a, r"
    val graph2 = fullSpace.base.cypher(query2)

    //    val result = graph1.graph.intersect(graph2.graph)
    //    result.records.data.count() should equal(0)
  }

  // TODO: Implement new syntax to make this work
  ignore("get a subgraph and query it") {
    val subgraphQ =
      """MATCH (u1:User)-[p:POSTED]->(t:Tweet)-[m:MENTIONED]->(u2:User)
        |WHERE u2.name = 'Neo4j'
        |RETURN u1, p, t, m, u2
      """.stripMargin

    val result = fullSpace.base.cypher(subgraphQ)

    val usernamesQ = "MATCH (u:User) RETURN u.name"

    val graph = result.namedGraph("someName") match {
      case Some(g) => g
      case None => fail("graph 'someName' not found")
    }
    val records = graph.cypher(usernamesQ).records

    records.show()
    // TODO: assertions
  }

  implicit class RichRecords(records: SparkCypherRecords) {
    def shouldHaveSize(size: Int) = {
      import org.opencypher.spark_legacy.impl.util._

      val tuples = records.data.collect().toSeq.map { r =>
        val cells = records.header.slots.map { s =>
          r.get(s.index)
        }

        cells.asProduct
      }

      tuples.size shouldBe size

      new {
        def andContain(contents: Product): Unit = {
          tuples should contain(contents)
        }

        def andContain(contents: Any): Unit = andContain(Tuple1(contents))
      }
    }
  }

  private val smallSchema = Schema.empty
    .withRelationshipKeys("ATTENDED")("guests" -> CTInteger, "comments" -> CTString.nullable)
    .withNodeKeys("User")("id" -> CTInteger.nullable, "text" -> CTString.nullable, "country" -> CTString.nullable, "city" -> CTString.nullable)
    .withNodeKeys("Meetup")("id" -> CTInteger.nullable, "city" -> CTString.nullable, "country" -> CTString.nullable)
    .withNodeKeys("Graph")("title" -> CTString.nullable, "updated" -> CTInteger.nullable)
    .withNodeKeys("Event")("time" -> CTInteger.nullable, "link" -> CTAny.nullable)

  private def initSmallSpace(schema: Schema = smallSchema,
                             nodeQ: String = "MATCH (a)-[:ATTENDED]->(b) UNWIND [a, b] AS n RETURN DISTINCT n",
                             relQ: String = "MATCH ()-[r:ATTENDED]->() RETURN r") = {
    SparkGraphSpace.fromNeo4j(nodeQ, relQ, schema)
  }

  private lazy val smallSpace = initSmallSpace()

  private lazy val fullSpace = SparkGraphSpace.fromNeo4j("MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r")
}
