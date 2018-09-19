/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.api.io.neo4j.sync

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.impl.acceptance.DefaultGraphInit
import org.opencypher.spark.impl.table.SparkTable
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.CAPSNeo4jServerFixture

import scala.collection.JavaConverters._

class Neo4JGraphMergeTest extends CAPSTestSuite with CAPSNeo4jServerFixture with DefaultGraphInit {

  override def dataFixture: String = ""

  val entityKeys: EntityKeys = EntityKeys(Map("N" -> Set("id")), Map("R" -> Set("id")))

  val initialGraph: RelationalCypherGraph[SparkTable.DataFrameTable] = initGraph(
    """
      |CREATE (s:N {id: 1, foo: "bar"})
      |CREATE (e:N:M {id: 2})
      |CREATE (s)-[r:R {id: 1}]->(e)
    """.stripMargin)

  override def afterEach(): Unit = {
    neo4jConfig.withSession { session =>
      session.run("MATCH (n) DETACH DELETE n").consume()
      val constraints = session.run("CALL db.constraints").list().asScala.map(_.get(0).asString)
      val regexp = """CONSTRAINT ON (.+) ASSERT \(?(.+?)\)? IS NODE KEY""".r

      // TODO remove workaround once it's fixed in Neo4j
      constraints.map {
        case regexp(label, keys) => s"DROP CONSTRAINT ON $label ASSERT ($keys) IS NODE KEY"
        case c => s"DROP $c"
      }.foreach(session.run(_).consume())
      session.run("MATCH (n) DETACH DELETE n").consume()

      session
        .run("CALL db.indexes YIELD description")
        .list().asScala
        .map(_.get(0).asString)
        .map(i => s"DROP $i")
        .foreach(session.run(_).consume())
    }
    super.afterEach()
  }

  describe("merging into the entire graph") {
    it("can do basic Neo4j merge") {
      Neo4jGraphMerge.createIndexes(neo4jConfig, entityKeys)
      Neo4jGraphMerge.merge(initialGraph, neo4jConfig, entityKeys)

      val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig).graph(entireGraphName)

      readGraph.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "foo" -> "bar", "labels" -> Seq("N")),
        CypherMap("id" -> 2, "foo" -> null, "labels" -> Seq("M", "N"))
      ))

      readGraph.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "mid" -> 2)
      ))

      // Do not change a graph when the same graph is merged as a delta
      Neo4jGraphMerge.merge(initialGraph, neo4jConfig, entityKeys)
      val graphAfterSameMerge = Neo4jPropertyGraphDataSource(neo4jConfig)
        .graph(entireGraphName)

      graphAfterSameMerge.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "foo" -> "bar", "labels" -> Seq("N")),
        CypherMap("id" -> 2, "foo" -> null, "labels" -> Seq("M", "N"))
      ))

      graphAfterSameMerge.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "mid" -> 2)
      ))

      // merge a delta
      val delta = initGraph(
        """
          |CREATE (s:N {id: 1, foo: "baz", bar: 1})
          |CREATE (e:N {id: 2})
          |CREATE (s)-[r:R {id: 1, foo: 1}]->(e)
          |CREATE (s)-[r:R {id: 2}]->(e)
        """.stripMargin)
      Neo4jGraphMerge.merge(delta, neo4jConfig, entityKeys)
      val graphAfterDeltaSync = Neo4jPropertyGraphDataSource(neo4jConfig)
        .graph(entireGraphName)

      graphAfterDeltaSync.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, n.bar as bar, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "foo" -> "baz", "bar" -> 1, "labels" -> Seq("N")),
        CypherMap("id" -> 2, "foo" -> null, "bar" -> null, "labels" -> Seq("M", "N"))
      ))

      graphAfterDeltaSync.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, r.foo as foo, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "foo" -> 1, "mid" -> 2),
        CypherMap("nid" -> 1, "id" -> 2, "foo" -> null, "mid" -> 2)
      ))
    }

    it("merges when using the same entity key for all labels") {
      val keys = EntityKeys(Map("N" -> Set("id"), "M" -> Set("id")), Map("R" -> Set("id")))
      Neo4jGraphMerge.createIndexes(neo4jConfig, keys)
      val graphName = GraphName("graph")

      val graph = initGraph(
        """
          |CREATE (s:N {id: 1, foo: "bar"})
          |CREATE (e:N:M {id: 2 })
          |CREATE (f:M {id: 3})
          |CREATE (s)-[r:R {id: 1}]->(e)
        """.stripMargin)

      Neo4jGraphMerge.merge(graph, neo4jConfig, keys)

      val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig, entireGraphName = graphName).graph(graphName)

      readGraph.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "foo" -> "bar", "labels" -> Seq("N")),
        CypherMap("id" -> 2, "foo" -> null, "labels" -> Seq("M", "N")),
        CypherMap("id" -> 3, "foo" -> null, "labels" -> Seq("M"))
      ))
    }

    it("merges when using multiple entity keys with different names") {
      val keys = EntityKeys(Map("N" -> Set("nId"), "M" -> Set("mId")), Map("R" -> Set("id")))
      Neo4jGraphMerge.createIndexes(neo4jConfig, keys)
      val graphName = GraphName("graph")

      val graph = initGraph(
        """
          |CREATE (s:N {nId: 1, foo: "bar"})
          |CREATE (e:N:M {nId: 2, mId: 3 })
          |CREATE (f:M {mId: 2})
          |CREATE (s)-[r:R {id: 1}]->(e)
        """.stripMargin)

      Neo4jGraphMerge.merge(graph, neo4jConfig, keys)

      val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig, entireGraphName = graphName).graph(graphName)

      readGraph.cypher("MATCH (n) RETURN n.nId as nId, n.mId as mId, n.foo as foo, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("nId" -> 1, "mId" -> null, "foo" -> "bar", "labels" -> Seq("N")),
        CypherMap("nId" -> 2, "mId" -> 3, "foo" -> null, "labels" -> Seq("M", "N")),
        CypherMap("nId" -> null, "mId" -> 2, "foo" -> null, "labels" -> Seq("M"))
      ))
    }

    it("creates indexes correctly") {
      val entityKeys = EntityKeys(
        Map(
          "N" -> Set("foo", "bar"),
          "M" -> Set("baz")
        ),
        Map(
          "REL" -> Set("a")
        )
      )

      Neo4jGraphMerge.createIndexes(neo4jConfig, entityKeys)

      neo4jConfig.cypher("CALL db.constraints YIELD description").toSet should equal(Set(
        Map("description" -> new CypherString("CONSTRAINT ON ( n:N ) ASSERT (n.foo, n.bar) IS NODE KEY")),
        Map("description" -> new CypherString("CONSTRAINT ON ( m:M ) ASSERT m.baz IS NODE KEY"))
      ))

      neo4jConfig.cypher("CALL db.indexes YIELD description").toSet should equal(Set(
        Map("description" -> new CypherString(s"INDEX ON :N($metaPropertyKey)")),
        Map("description" -> new CypherString(s"INDEX ON :N(foo, bar)")),
        Map("description" -> new CypherString(s"INDEX ON :M($metaPropertyKey)")),
        Map("description" -> new CypherString(s"INDEX ON :M(baz)"))
      ))
    }
  }

  describe("merging into subgraphs") {
    it("merges subgraphs") {
      val subGraphName = GraphName("foo")

      Neo4jGraphMerge.createIndexes(subGraphName, neo4jConfig, entityKeys)
      Neo4jGraphMerge.merge(subGraphName, initialGraph, neo4jConfig, entityKeys)

      val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig).graph(subGraphName)

      readGraph.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "foo" -> "bar", "labels" -> Seq("N")),
        CypherMap("id" -> 2, "foo" -> null, "labels" -> Seq("M", "N"))
      ))

      readGraph.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "mid" -> 2)
      ))

      // Do not change a graph when the same graph is synced as a delta
      Neo4jGraphMerge.merge(initialGraph, neo4jConfig, entityKeys)
      val graphAfterSameSync = Neo4jPropertyGraphDataSource(neo4jConfig).graph(subGraphName)

      graphAfterSameSync.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "foo" -> "bar", "labels" -> Seq("N")),
        CypherMap("id" -> 2, "foo" -> null, "labels" -> Seq("M", "N"))
      ))

      graphAfterSameSync.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "mid" -> 2)
      ))

      // Sync a delta
      val delta = initGraph(
        """
          |CREATE (s:N {id: 1, foo: "baz", bar: 1})
          |CREATE (e:N {id: 2})
          |CREATE (s)-[r:R {id: 1, foo: 1}]->(e)
          |CREATE (s)-[r:R {id: 2}]->(e)
        """.stripMargin)
      Neo4jGraphMerge.merge(subGraphName, delta, neo4jConfig, entityKeys)
      val graphAfterDeltaSync = Neo4jPropertyGraphDataSource(neo4jConfig).graph(subGraphName)

      graphAfterDeltaSync.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, n.bar as bar, labels(n) as labels").records.toMaps should equal(Bag(
        CypherMap("id" -> 1, "foo" -> "baz", "bar" -> 1, "labels" -> Seq("N")),
        CypherMap("id" -> 2, "foo" -> null, "bar" -> null, "labels" -> Seq("M", "N"))
      ))

      graphAfterDeltaSync.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, r.foo as foo, m.id as mid").records.toMaps should equal(Bag(
        CypherMap("nid" -> 1, "id" -> 1, "foo" -> 1, "mid" -> 2),
        CypherMap("nid" -> 1, "id" -> 2, "foo" -> null, "mid" -> 2)
      ))
    }

    it("creates indexes correctly") {
      val newEntityKeys = EntityKeys(
        Map(
          "N" -> Set("foo", "bar"),
          "M" -> Set("baz")
        ),
        Map(
          "REL" -> Set("a")
        )
      )

      val subGraphName = GraphName("myGraph")
      Neo4jGraphMerge.createIndexes(subGraphName, neo4jConfig, newEntityKeys)

      neo4jConfig.cypher("CALL db.constraints YIELD description").toSet shouldBe empty

      neo4jConfig.cypher("CALL db.indexes YIELD description").toSet should equal(Set(
        Map("description" -> new CypherString(s"INDEX ON :${subGraphName.metaLabelForSubgraph}($metaPropertyKey)")),
        Map("description" -> new CypherString(s"INDEX ON :N(foo, bar)")),
        Map("description" -> new CypherString(s"INDEX ON :M(baz)"))
      ))
    }
  }

  describe("error handling") {
    it("should throw an error if node key is missing") {
      a[SchemaException] should be thrownBy Neo4jGraphMerge.merge(initialGraph, neo4jConfig, EntityKeys(Map.empty))
    }
  }
}
