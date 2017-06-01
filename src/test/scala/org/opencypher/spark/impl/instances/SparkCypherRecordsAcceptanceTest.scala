package org.opencypher.spark.impl.instances

import org.apache.spark.sql.Row
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark.SparkGraphSpace
import org.opencypher.spark.api.types.{CTAny, CTInteger, CTString}
import org.opencypher.spark.impl.instances.spark.cypher._
import org.opencypher.spark.impl.syntax.cypher._
import org.opencypher.spark.{StdTestSuite, TestSession}

class SparkCypherRecordsAcceptanceTest extends StdTestSuite with TestSession.Fixture {

  test("label scan and project") {
    val records = smallSpace.base.cypher("MATCH (a:User) RETURN a.text").records

    records.data.count() shouldBe 1806
    records.data.collect().toSet.map((r: Row) => r.get(0)) should contain("Application Developer")
    records.header.slots.size shouldBe 1
    records.header.slots.head.content.cypherType shouldBe CTString.nullable
    records.header.slots.head.content.key should equal(Var("a.text")(CTString.nullable))
  }

  test("expand and project") {
    val records = smallSpace.base.cypher("MATCH (a:User)-[r]->(m:Meetup) RETURN a.country, m.id").records

    records.data.count() shouldBe 4832
    records.header.slots.size shouldBe 2
    records.header.slots(0).content.cypherType shouldBe CTString.nullable
    records.header.slots(0).content.key should equal(Var("a.country")(CTString.nullable))
    records.header.slots(1).content.cypherType shouldBe CTInteger.nullable
    records.header.slots(1).content.key should equal(Var("m.id")(CTInteger.nullable))
  }

  test("expand and project on full graph") {
    val records = fullSpace.base.cypher("MATCH (g:Graph)-[r:CONTAINED]->(e:Event) RETURN g.key, e.title").records

    val start = System.currentTimeMillis()
    val rows = records.data.collect()

    rows.length shouldBe 25
    rows.toSet.exists { r =>
      r.getString(0) == "GraphDB-Sydney"
    } shouldBe true
    rows.toSet.exists { r =>
      r.getString(1) == "May Neo4J/graphdb meetup"
    } shouldBe true
  }

  test("filter rels on property") {
    val query = "MATCH (a:User)-[r:ATTENDED]->() WHERE r.response = 'no' RETURN a, r"

    val graph = fullSpace.base.cypher(query)

    graph.records.data.count() shouldBe 1173
  }

  test("expand and project on full graph, three properties") {
    val query = "MATCH (t:Tweet)-[:MENTIONED]->(l:User) RETURN t.text, l.location, l.followers"

    val records = fullSpace.base.cypher(query).records

    val rows = records.data.collect().toSeq
    val slots = records.header.slotsFor("t.text", "l.location", "l.followers")

    rows.length shouldBe 815
    rows.exists { r =>
      r.getString(slots.head.index) == "@Khanoisseur @roygrubb @Parsifalssister @Rockmedia a perfect problem for a graph database like #neo4j"
    } shouldBe true
    rows.exists { r =>
      r.getString(slots(1).index) == "Szeged and Gent"
    } shouldBe true
    rows.exists { r =>
      !r.isNullAt(slots(2).index) && r.getLong(slots(2).index) == 83266l
    } shouldBe true
  }

  test("handle properties with same key and different type between labels") {
    val space = initSmallSpace(Schema.empty
      .withNodeKeys("Channel")("id" -> CTString.nullable)
      .withNodeKeys("GitHub")("id" -> CTInteger.nullable), "MATCH (n) RETURN n", "RETURN 1 LIMIT 0")

    val channels = space.base.cypher("MATCH (c:Channel) RETURN c.id").records

    channels.data.count() shouldBe 78
    channels.header.slots(0).content.cypherType shouldBe CTString.nullable

    val githubs = space.base.cypher("MATCH (g:GitHub) RETURN g.id").records

    githubs.data.count() shouldBe 365
    githubs.header.slots(0).content.cypherType shouldBe CTInteger.nullable
  }

  test("property filter in small space") {
    val records = smallSpace.base.cypher("MATCH (t:User) WHERE t.country = 'ca' RETURN t.city").records

    val results = records.toDF().collect().toSeq.map {
      (r: Row) => r.getString(0)
    }

    results.size shouldBe 38
    results should contain("Vancouver")
  }

  test("multiple hops of expand with different reltypes") {
    val query = "MATCH (u1:User)-[p:POSTED]->(t:Tweet)-[m:MENTIONED]->(u2:User) RETURN u1.name, u2.name, t.text"

    val records = fullSpace.base.cypher(query).records

    val slots = records.header.slotsFor("u1.name", "u2.name", "t.text")
    val tuples = records.toDF().collect().toSeq.map { r =>
      (r.get(slots.head.index), r.get(slots(1).index), r.get(slots(2).index))
    }

    tuples.size shouldBe 79
    val tuple = ("Brendan Madden", "Tom Sawyer Software",
      "#tsperspectives 7.6 is 15% faster with #neo4j Bolt support. https://t.co/1xPxB9slrB @TSawyerSoftware #graphviz")
    tuples should contain(tuple)
  }

  test("multiple hops of expand with possible reltype conflict") {
    val query = "MATCH (u1:User)-[r1:POSTED]->(t:Tweet)-[r2]->(u2:User) RETURN u1.name, u2.name, t.text"

    val records = fullSpace.base.cypher(query).records

    val slots = records.header.slotsFor("u1.name", "u2.name", "t.text")
    val tuples = records.toDF().collect().toSeq.map { r =>
      (r.get(slots.head.index), r.get(slots(1).index), r.get(slots(2).index))
    }

    tuples.size shouldBe 79
    val tuple = ("Brendan Madden", "Tom Sawyer Software",
      "#tsperspectives 7.6 is 15% faster with #neo4j Bolt support. https://t.co/1xPxB9slrB @TSawyerSoftware #graphviz")
    tuples should contain(tuple)
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
