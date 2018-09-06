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
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.impl.acceptance.DefaultGraphInit
import org.opencypher.spark.impl.table.SparkTable
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.CAPSNeo4jServerFixture

import scala.collection.JavaConverters._

class Neo4jSyncTest extends CAPSTestSuite with CAPSNeo4jServerFixture with DefaultGraphInit {

  override def dataFixture: String = ""

  val entityKeys: EntityKeys = EntityKeys(Map(Set("N") -> Set("id")), Map("R" -> Set("id")))
  val entireGraphName: GraphName = GraphName("graph")

  val initialGraph: RelationalCypherGraph[SparkTable.DataFrameTable] = initGraph(
    """
      |CREATE (s:N {id: 1, foo: "bar"})
      |CREATE (e:N {id: 2})
      |CREATE (s)-[r:R {id: 1}]->(e)
    """.stripMargin)

  override def afterEach(): Unit = {
    neo4jConfig.withSession { session =>
      session.run("MATCH (n) DETACH DELETE n").consume()
      val constraints = session.run("CALL db.constraints").list().asScala.map(_.get(0).asString)
      val regexp = """CONSTRAINT ON (.+) ASSERT (.+) IS NODE KEY""".r

      // TODO remove workaround once it's fixed in Neo4j
      val constraintString = constraints.map {
        case regexp(labels, keys) => s"DROP CONSTRAINT ON $labels ASSERT ($keys) IS NODE KEY"
        case c => s"DROP $c"
      }.mkString("\n")
      session.run(constraintString).consume()
      session.run("MATCH (n) DETACH DELETE n").consume()
    }
    super.afterEach()
  }

  it("can do basic Neo4j syncing with merges") {
//    Neo4jSync.createIndexes(neo4jConfig, entityKeys)
    Neo4jSync.merge(initialGraph, neo4jConfig, entityKeys)

    val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig, entireGraphName = entireGraphName).graph(entireGraphName)

    readGraph.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, labels(n) as labels").records.toMaps should equal(Bag(
      CypherMap("id" -> 1, "foo" -> "bar", "labels" -> Seq("N")),
      CypherMap("id" -> 2, "foo" -> null, "labels" -> Seq("N"))
    ))

    readGraph.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, m.id as mid").records.toMaps should equal(Bag(
      CypherMap("nid" -> 1, "id" -> 1, "mid" -> 2)
    ))

    // Do not change a graph when the same graph is synced as a delta
    Neo4jSync.merge(initialGraph, neo4jConfig, entityKeys)
    val graphAfterSameSync = Neo4jPropertyGraphDataSource(neo4jConfig, entireGraphName = entireGraphName)
      .graph(entireGraphName)

    graphAfterSameSync.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, labels(n) as labels").records.toMaps should equal(Bag(
      CypherMap("id" -> 1, "foo" -> "bar", "labels" -> Seq("N")),
      CypherMap("id" -> 2, "foo" -> null, "labels" -> Seq("N"))
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
    Neo4jSync.merge(delta, neo4jConfig, entityKeys)
    val graphAfterDeltaSync = Neo4jPropertyGraphDataSource(neo4jConfig, entireGraphName = entireGraphName)
      .graph(entireGraphName)

    graphAfterDeltaSync.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, n.bar as bar, labels(n) as labels").records.toMaps should equal(Bag(
      CypherMap("id" -> 1, "foo" -> "baz", "bar" -> 1, "labels" -> Seq("N")),
      CypherMap("id" -> 2, "foo" -> null, "bar" -> null, "labels" -> Seq("N"))
    ))

    graphAfterDeltaSync.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, r.foo as foo, m.id as mid").records.toMaps should equal(Bag(
      CypherMap("nid" -> 1, "id" -> 1, "foo" -> 1, "mid" -> 2),
      CypherMap("nid" -> 1, "id" -> 2, "foo" -> null, "mid" -> 2)
    ))
  }

  it("can do basic Neo4j sub-graph syncing with merges") {
    val subGraphName = GraphName("foo")

//    Neo4jSync.createIndexes(subGraphName, neo4jConfig, entityKeys)
    Neo4jSync.merge(subGraphName, initialGraph, neo4jConfig, entityKeys)

    val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig).graph(subGraphName)

    readGraph.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, labels(n) as labels").records.toMaps should equal(Bag(
      CypherMap("id" -> 1, "foo" -> "bar", "labels" -> Seq("N")),
      CypherMap("id" -> 2, "foo" -> null, "labels" -> Seq("N"))
    ))

    readGraph.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, m.id as mid").records.toMaps should equal(Bag(
      CypherMap("nid" -> 1, "id" -> 1, "mid" -> 2)
    ))

    // Do not change a graph when the same graph is synced as a delta
    Neo4jSync.merge(initialGraph, neo4jConfig, entityKeys)
    val graphAfterSameSync = Neo4jPropertyGraphDataSource(neo4jConfig).graph(subGraphName)

    graphAfterSameSync.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, labels(n) as labels").records.toMaps should equal(Bag(
      CypherMap("id" -> 1, "foo" -> "bar", "labels" -> Seq("N")),
      CypherMap("id" -> 2, "foo" -> null, "labels" -> Seq("N"))
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
    Neo4jSync.merge(subGraphName, delta, neo4jConfig, entityKeys)
    val graphAfterDeltaSync = Neo4jPropertyGraphDataSource(neo4jConfig).graph(subGraphName)

    graphAfterDeltaSync.cypher("MATCH (n) RETURN n.id as id, n.foo as foo, n.bar as bar, labels(n) as labels").records.toMaps should equal(Bag(
      CypherMap("id" -> 1, "foo" -> "baz", "bar" -> 1, "labels" -> Seq("N")),
      CypherMap("id" -> 2, "foo" -> null, "bar" -> null, "labels" -> Seq("N"))
    ))

    graphAfterDeltaSync.cypher("MATCH (n)-[r]->(m) RETURN n.id as nid, r.id as id, r.foo as foo, m.id as mid").records.toMaps should equal(Bag(
      CypherMap("nid" -> 1, "id" -> 1, "foo" -> 1, "mid" -> 2),
      CypherMap("nid" -> 1, "id" -> 2, "foo" -> null, "mid" -> 2)
    ))
  }

}
