/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTString
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.test.support.Bag
import org.opencypher.okapi.ir.test.support.Bag._
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.test.support.creation.caps.{CAPSScanGraphFactory, CAPSTestGraphFactory}

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSTestGraphFactory = CAPSScanGraphFactory

  def testGraph1 = initGraph("CREATE (:Person {name: 'Mats'})")

  def testGraph2 = initGraph("CREATE (:Person {name: 'Phil'})")

  def testGraph3 = initGraph("CREATE (:Car {type: 'Toyota'})")

  def testGraphRels = initGraph(
    """|CREATE (mats:Person {name: 'Mats'})
       |CREATE (max:Person {name: 'Max'})
       |CREATE (max)-[:HAS_SIMILAR_NAME]->(mats)
    """.stripMargin)

  it("should construct a node property from a matched node") {
    val query =
      """|MATCH (m)
         |CONSTRUCT
         |  NEW (a :A { name: m.name})
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty
    result.getGraph.schema.labels should equal(Set("A"))
    result.getGraph.schema should equal(Schema.empty.withNodePropertyKeys("A")("name" -> CTString).asCaps)
    result.getGraph.cypher("MATCH (a:A) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
      CypherMap("a.name" -> "Mats")
    ))
  }
}
