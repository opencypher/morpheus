/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.api.io.neo4j

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.neo4j.io.testing.Neo4jServerFixture
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.api.CypherGraphSources
import org.opencypher.spark.impl.acceptance.DefaultGraphInit
import org.opencypher.spark.testing.CAPSTestSuite

class Neo4jPropertyGraphDataSourceWriteTest
  extends CAPSTestSuite
    with Neo4jServerFixture
    with DefaultGraphInit{

  it("can write a graph to Neo4j") {
    val g = initGraph(
      """
        |CREATE (a:A {val: 1})-[:REL {val: 3}]->(b:B {val: 2})
        |CREATE (b)-[:REL {val: 4}]->(a)
      """.stripMargin)

    val dataSource = CypherGraphSources.neo4j(neo4jConfig)

    dataSource.store(GraphName("g1"), g)

    neo4jConfig.cypherWithNewSession("MATCH (n)-[r]->(m) RETURN n.val, r.val, m.val").map(x => CypherMap(x.toSeq:_*)).toBag should equal(Bag(
      CypherMap("n.val" -> 1, "r.val" -> 3, "m.val" -> 2),
      CypherMap("n.val" -> 2, "r.val" -> 4, "m.val" -> 1)
    ))
  }

  override def dataFixture: String = ""
}
