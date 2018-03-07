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

import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintPhysicalPlan
import org.opencypher.spark.test.support.creation.caps.{CAPSScanGraphFactory, CAPSTestGraphFactory}

import org.opencypher.spark.impl.CAPSConverters._

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSTestGraphFactory = CAPSScanGraphFactory

  def testGraph1 = initGraph("CREATE (:Person {name: 'Mats'})")

  def testGraph2 = initGraph("CREATE (:Person {name: 'Phil'})")

  def testGraph3 = initGraph("CREATE (:Car {type: 'Toyota'})")

  PrintLogicalPlan.set()

  PrintPhysicalPlan.set()

  it("should construct a graph") {
    val query =
      """|CONSTRUCT {
         |  CREATE (:A)-[:KNOWS]->(:B)
         |}
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty

    result.getGraph.schema.labels should equal(Set("A", "B"))
    result.getGraph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.getGraph.nodes("n").iterator.length should equal(2)
  }

  it("should CONSTRUCT a graph with multiple connected CREATE clauses") {
    val query =
      """|CONSTRUCT {
         |  CREATE (a:A)-[:KNOWS]->(b:B)
         |  CREATE (b:B)-[:KNOWS]->(c:C)
         |}
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty

    result.getGraph.schema.labels should equal(Set("A", "B", "C"))
    result.getGraph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.getGraph.nodes("n").iterator.length should equal(3)
  }

  it("should CONSTRUCT a graph with multiple unconnected CREATE clauses") {
    val query =
      """|CONSTRUCT {
         |  CREATE (a:A)-[:KNOWS]->(b:B)
         |  CREATE (c:C)-[:KNOWS]->(d:D)
         |}
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty

    result.getGraph.schema.labels should equal(Set("A", "B", "C", "D"))
    result.getGraph.schema.relationshipTypes should equal(Set("KNOWS"))
    result.getGraph.nodes("n").iterator.length should equal(4)
    result.getGraph.nodes("n").capsRecords.show
  }

  // TODO: monotonically_increasing_id seems to always return 0
  ignore("should CONSTRUCT a graph with multiple unconnected anonymous CREATE clauses") {
    val query =
      """|CONSTRUCT {
         |  CREATE (:A)
         |  CREATE (:B)
         |}
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty

    result.getGraph.schema.labels should equal(Set("A", "B"))
    result.getGraph.schema.relationshipTypes should equal(Set.empty)

    result.getGraph.nodes("n").records.asCaps.toDF.show

    result.getGraph.nodes("n").iterator.length should equal(2)
  }

  // TODO: implement constructing properties
  ignore("should construct a node property") {
    val query =
      """|CONSTRUCT {
         |  CREATE (:A {name: 'Donald'})
         |}
         |RETURN GRAPH""".stripMargin

    val result = testGraph1.cypher(query)

    result.getRecords.toMaps shouldBe empty

    result.getGraph.schema.labels should equal(Set("A"))
    result.getGraph.nodes("n").iterator.length should equal(1)
  }

}
