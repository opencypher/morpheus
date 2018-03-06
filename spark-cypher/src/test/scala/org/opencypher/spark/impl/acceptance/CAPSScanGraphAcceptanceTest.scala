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
 */
package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.spark.test.support.creation.caps.{CAPSScanGraphFactory, CAPSTestGraphFactory}

import scala.collection.immutable.Bag
import org.opencypher.spark.impl.CAPSConverters._

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSTestGraphFactory = CAPSScanGraphFactory

  def testGraph1 = initGraph("CREATE (:Person {name: 'Mats'})")

  def testGraph2 = initGraph("CREATE (:Person {name: 'Phil'})")

  def testGraph3 = initGraph("CREATE (:Car {type: 'Toyota'})")

  it("should return a graph") {
    val query =
      """RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)
    result.getRecords.toMaps shouldBe empty

    result.asCaps.getGraph shouldMatch testGraph1
  }

  it("should switch to another graph and then return it") {
    caps.store(GraphName("graph2"), testGraph2)
    val query =
      """USE GRAPH graph2
        |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)
    result.getRecords.toMaps shouldBe empty
    result.asCaps.getGraph shouldMatch testGraph2
  }

  it("can select a source graph to match data from") {
    caps.store(GraphName("graph1"), testGraph1)
    caps.store(GraphName("graph2"), testGraph2)

    val query =
      """USE GRAPH graph2
        |MATCH (n:Person)
        |RETURN n.name AS name""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("name" -> "Phil")
      ))
  }

  it("matches from different graphs") {
    caps.store(GraphName("graph1"), testGraph1)
    caps.store(GraphName("graph2"), testGraph2)
    caps.store(GraphName("graph3"), testGraph3)

    val query =
      """USE GRAPH graph2
        |MATCH (n:Person)
        |WITH n.name AS name
        |USE GRAPH graph3
        |MATCH (c:Car)
        |RETURN name, c.type AS car""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps should equal(
      Bag(
        CypherMap("name" -> "Phil", "car" -> "Toyota")
      ))
  }

}
