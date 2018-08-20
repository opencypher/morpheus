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
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.impl.acceptance.DefaultGraphInit
import org.opencypher.spark.impl.table.SparkTable
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.CAPSNeo4jServerFixture

class Neo4jSyncTest extends CAPSTestSuite with CAPSNeo4jServerFixture with DefaultGraphInit {

  override def dataFixture: String = ""

  it("can do basic Neo4j syncing with merges") {
    val entityKeys: EntityKeys = EntityKeys(Map(Set("N") -> Set("id")), Map("R" -> Set("id")))
    val entireGraphName: GraphName = GraphName("graph")

    val initialGraph: RelationalCypherGraph[SparkTable.DataFrameTable] = initGraph(
      """
        |CREATE (s:N {id: 1})
        |CREATE (e:N {id: 2})
        |CREATE (s)-[r:R {id: 1}]->(e)
      """.stripMargin)

    // Write an initial graph to Neo4j
    Neo4jSync.merge(initialGraph, neo4jConfig, entityKeys)
    val readGraph = Neo4jPropertyGraphDataSource(neo4jConfig, entireGraphName = entireGraphName)
      .graph(entireGraphName)
    val q =
      """
        |MATCH ()-[r]->()
        |RETURN r.id
      """.stripMargin

    val records = readGraph.cypher(q).records.toMaps
    records should equal(Bag(
      CypherMap("r.id" -> 1)
    ))

    // Do not change a graph when the same graph is synced as a delta
    Neo4jSync.merge(initialGraph, neo4jConfig, entityKeys)
    val graphAfterSameSync = Neo4jPropertyGraphDataSource(neo4jConfig, entireGraphName = entireGraphName)
      .graph(entireGraphName)

    val recordsAfterSameSync = graphAfterSameSync.cypher(q).records.toMaps
    recordsAfterSameSync should equal(Bag(
      CypherMap("r.id" -> 1)
    ))

    // Sync a delta
    val delta = initGraph(
      """
        |CREATE (s:N {id: 1, bar: 1})
        |CREATE (e:N {id: 2})
        |CREATE (s)-[r:R {id: 1, foo: 1}]->(e)
      """.stripMargin)
    Neo4jSync.merge(delta, neo4jConfig, entityKeys)
    val graphAfterDeltaSync = Neo4jPropertyGraphDataSource(neo4jConfig, entireGraphName = entireGraphName)
      .graph(entireGraphName)

    val deltaQuery =
      """
        |MATCH (s)-[r]->(e)
        |RETURN s.bar, r.foo, e.bar
      """.stripMargin

    val recordsAfterDeltaSync = graphAfterDeltaSync.cypher(deltaQuery).records.toMaps
    recordsAfterDeltaSync should equal(Bag(
      CypherMap("s.bar" -> 1, "r.foo" -> 1, "e.bar" -> null)
    ))
  }

}
