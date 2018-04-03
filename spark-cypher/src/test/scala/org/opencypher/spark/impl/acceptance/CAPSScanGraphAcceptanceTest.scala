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

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.value.{CAPSNode, CypherValue}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.test.support.Bag
import org.opencypher.okapi.ir.test.support.Bag._
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.test.support.creation.caps.{CAPSScanGraphFactory, CAPSTestGraphFactory}
import org.opencypher.spark.impl.DataFrameOps._

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSTestGraphFactory = CAPSScanGraphFactory

  def testGraph1 = initGraph("CREATE (:Person {name: 'Mats'})")

  def testGraph2 = initGraph("CREATE (:Person {name: 'Phil'})")

  def uselessGraph = initGraph("CREATE ()")

  def testGraphRels = initGraph(
    """|CREATE (mats:Person {name: 'Mats'})
       |CREATE (max:Person {name: 'Max'})
       |CREATE (max)-[:HAS_SIMILAR_NAME]->(mats)
    """.stripMargin)

  it("very simple") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a)
        |RETURN GRAPH
      """.stripMargin

    val graph = caps.cypher(query).getGraph

    graph.schema should equal(Schema.empty.withNodePropertyKeys(Set.empty[String]).asCaps)
    graph.asCaps.tags should equal(Set(0))
    graph.nodes("n").collect.toBag should equal(Bag(
      CypherMap("n" -> CAPSNode(0))
    ))
  }

  it("construct match construct") {
    caps.store(GraphName("g1"), testGraphRels)
    val query =
      """
        |FROM GRAPH g1
        |MATCH (a)
        |CONSTRUCT // generated qgn
        |  CLONE a
        |MATCH (b)
        |CONSTRUCT
        |  ON g1
        |  CLONE b
        |RETURN GRAPH
      """.stripMargin

    PrintLogicalPlan.set

    val graph = caps.cypher(query).getGraph

    graph.schema should equal(testGraphRels.schema)
    graph.asCaps.tags should equal(Set(0, 1))
    graph.nodes("n").collect.toBag should equal(Bag(
      CypherMap("n" -> CAPSNode(0, Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(1, Set("Person"), CypherMap("name" -> "Max"))),
      CypherMap("n" -> CAPSNode(0L.setTag(1), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(1L.setTag(1), Set("Person"), CypherMap("name" -> "Max")))
    ))
  }

  it("very simple 2") {
    caps.store(GraphName("g1"), testGraph1)
    caps.store(GraphName("g2"), testGraph2)
    val query =
      """
        |CONSTRUCT ON g1, g2
        |RETURN GRAPH
      """.stripMargin

    val graph = caps.cypher(query).getGraph

    graph.schema should equal((testGraph1.schema ++ testGraph2.schema).asCaps)
    graph.asCaps.tags should equal(Set(0, 1))
    graph.nodes("n").collect.toBag should equal(Bag(
      CypherMap("n" -> CAPSNode(0, Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(0L.setTag(1), Set("Person"), CypherMap("name" -> "Phil")))
    ))
  }

  it("somewhat complex") {
    caps.store(GraphName("g1"), testGraph1)
    caps.store(GraphName("g2"), testGraph2)
    val query =
      """
        |FROM GRAPH g1
        |MATCH (a:Person)
        |FROM GRAPH g2
        |MATCH (b:Person)
        |CONSTRUCT
        |  ON g2
        |  CLONE a, b
        |RETURN GRAPH
      """.stripMargin

    val graph = caps.cypher(query).getGraph

    graph.schema should equal(testGraph1.schema.asCaps)
    graph.asCaps.tags should equal(Set(0, 1))
    graph.nodes("n").collect.toBag should equal(Bag(
      CypherMap("n" -> CAPSNode(0L.setTag(1), Set("Person"), CypherMap("name" -> "Mats"))),
      CypherMap("n" -> CAPSNode(0L, Set("Person"), CypherMap("name" -> "Phil")))
    ))
  }

  it("CONSTRUCTS ON a relationship") {
    caps.store(GraphName("testGraphRels1"), testGraphRels)
    caps.store(GraphName("testGraphRels2"), testGraphRels)
    val query =
      """|FROM GRAPH testGraphRels1
         |MATCH (p1 :Person)-[r1]->(p2 :Person)
         |CONSTRUCT ON testGraphRels2
         |  CLONE p1, r1, p2
         |RETURN GRAPH""".stripMargin

    val result = caps.cypher(query).getGraph

    result.nodes("n").asCaps.data.show
    result.relationships("r").asCaps.data.show

//    result.schema should equal(testGraph1.schema.union(testGraph2.schema).withRelationshipPropertyKeys("KNOWS")().withTags(0, 1, 2).asCaps)
//    result.nodes("n").toMaps should equal(testGraph1.unionAll(testGraph2).nodes("n").toMaps)
//    result.relationships("r").toMapsWithCollectedEntities should equal(Bag(
//      CypherMap("r" -> CAPSRelationship(2251799813685248L, 0L, 1125899906842624L, "KNOWS")))
//    )
//    result.schema.toTagged.tags should equal(Set(0, 1, 2))
  }

  ignore("CONSTRUCT: cloning from different graphs") {
    def testGraphRels = initGraph(
      """|CREATE (mats:Person {name: 'Mats'})
         |CREATE (max:Person {name: 'Max'})
         |CREATE (max)-[:HAS_SIMILAR_NAME]->(mats)
      """.stripMargin)
    caps.store(GraphName("testGraphRels1"), testGraphRels)
    caps.store(GraphName("testGraphRels2"), testGraphRels)
    val query =
      """|FROM GRAPH testGraphRels1
         |MATCH (p1 :Person)-[r1]->(p2 :Person)
         |FROM GRAPH testGraphRels2
         |MATCH (p3 :Person)-[r2]->(p4 :Person)
         |CONSTRUCT
         |  CLONE p1, p2, p3, p4, r1, r2
         |RETURN GRAPH""".stripMargin

    val result = caps.cypher(query).getGraph

    result.nodes("n").asCaps.data.show
    result.relationships("r").asCaps.data.show
  }


}
