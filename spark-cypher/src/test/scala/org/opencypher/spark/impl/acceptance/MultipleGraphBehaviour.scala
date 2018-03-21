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
import org.opencypher.okapi.api.schema.{PropertyKeys, Schema}
import org.opencypher.okapi.api.types.{CTInteger, CTString}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.schema.TagSupport._
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.schema.CAPSSchema._

import scala.collection.immutable.Bag

trait MultipleGraphBehaviour {
  this: AcceptanceTest =>

  def multipleGraphBehaviour(initGraph: String => CAPSGraph): Unit = {
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
      result.getGraph.nodes("n").size should equal(2)
      result.getGraph.relationships("r").size should equal(1)
    }

    it("should CONSTRUCT a graph with multiple connected CREATE clauses") {
      val query =
        """|CONSTRUCT {
           |  CREATE (a:A)-[:KNOWS]->(b:B)
           |  CREATE (b)-[:KNOWS]->(c:C)
           |}
           |RETURN GRAPH""".stripMargin

      val result = testGraph1.cypher(query)

      result.getRecords.toMaps shouldBe empty
      result.getGraph.schema.labels should equal(Set("A", "B", "C"))
      result.getGraph.schema.relationshipTypes should equal(Set("KNOWS"))
      result.getGraph.nodes("n").size should equal(3)
      result.getGraph.relationships("r").size should equal(2)
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
      result.getGraph.nodes("n").size should equal(4)
      result.getGraph.relationships("r").size should equal(2)
    }

    it("should CONSTRUCT a graph with multiple unconnected anonymous CREATE clauses") {
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
      result.getGraph.nodes("n").size should equal(2)
      result.getGraph.relationships("r").size should equal(0)
    }

    it("should construct a node property from a matched node") {
      val query =
        """|MATCH (m)
           |CONSTRUCT {
           |  CREATE (a :A)
           |  SET a.name = m.name
           |}
           |RETURN GRAPH""".stripMargin

      val result = testGraph1.cypher(query)

      result.getRecords.toMaps shouldBe empty
      result.getGraph.schema.labels should equal(Set("A"))
      result.getGraph.schema should equal(Schema.empty.withNodePropertyKeys("A")("name" -> CTString).withTags(0, 1).asCaps)
      result.getGraph.cypher("MATCH (a:A) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
        CypherMap("a.name" -> "Mats")
      ))
    }

    it("should construct a node property from a literal") {
      val query =
        """|CONSTRUCT {
           |  CREATE (a :A)
           |  SET a.name = 'Donald'
           |}
           |RETURN GRAPH""".stripMargin

      val result = testGraph1.cypher(query)

      result.getRecords.toMaps shouldBe empty
      result.getGraph.schema.labels should equal(Set("A"))
      result.getGraph.schema should equal(Schema.empty.withNodePropertyKeys("A")("name" -> CTString).withTags(0, 1).asCaps)
      result.getGraph.cypher("MATCH (a:A) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
        CypherMap("a.name" -> "Donald")
      ))
    }

    it("should construct multiple properties") {
      val query =
        """|CONSTRUCT {
           |  CREATE (a :A:B)
           |  SET a.name = 'Donald'
           |  SET a.age = 100
           |}
           |RETURN GRAPH""".stripMargin

      val result = testGraph1.cypher(query)

      result.getRecords.toMaps shouldBe empty
      result.getGraph.schema.labels should equal(Set("A", "B"))
      result.getGraph.schema should equal(
        Schema.empty
          .withNodePropertyKeys(Set("A", "B"), PropertyKeys("name" -> CTString, "age" -> CTInteger))
            .withTags(0, 1)
          .asCaps)
      result.getGraph.cypher("MATCH (a:A) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
        CypherMap("a.name" -> "Donald")
      ))
    }

    it("should pick up labels of the outer match") {
      val query =
        """|MATCH (m:Person)
           |CONSTRUCT {
           |  CREATE (a~m)
           |}
           |RETURN GRAPH""".stripMargin

      val result = testGraph1.cypher(query)

      result.getRecords.toMaps shouldBe empty
      result.getGraph.schema.labels should equal(Set("Person"))
      result.getGraph.schema should equal(
        Schema.empty
          .withNodePropertyKeys(Set("Person"), PropertyKeys("name" -> CTString))
          .withTags(0, 1)
          .asCaps)
      result.getGraph.cypher("MATCH (a:Person) RETURN a.name").getRecords.iterator.toBag should equal(Bag(
        CypherMap("a.name" -> "Mats")
      ))
    }

    it("should construct a relationship") {
      val query =
        """|CONSTRUCT {
           |  CREATE ()-[r:FOO]->()
           |  SET r.val = 42
           |}
           |RETURN GRAPH""".stripMargin

      val result = testGraph1.cypher(query)

      result.getRecords.toMaps shouldBe empty
      result.getGraph.schema.relationshipTypes should equal(Set("FOO"))
      result.getGraph.schema should equal(Schema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("FOO", PropertyKeys("val" -> CTInteger)).withTags(0, 1).asCaps)
      result.getGraph.cypher("MATCH ()-[r]->() RETURN r.val").getRecords.iterator.toBag should equal(Bag(
        CypherMap("r.val" -> 42)
      ))
    }

    it("should tilde copy a relationship") {
      val query =
        """|CONSTRUCT {
           |  CREATE ()-[r:FOO]->()
           |  SET r.val = 42
           |}
           |MATCH ()-[s]->()
           |CONSTRUCT {
           |  CREATE ()-[t~s]->()
           |  SET t.name = 'Donald'
           |}
           |RETURN GRAPH""".stripMargin

      val result = testGraph1.cypher(query)

      result.getRecords.toMaps shouldBe empty
      result.getGraph.schema.relationshipTypes should equal(Set("FOO"))
      result.getGraph.schema should equal(Schema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("FOO", PropertyKeys("val" -> CTInteger, "name" -> CTString))
        .withTags(0, 1)
        .asCaps)
      result.getGraph.cypher("MATCH ()-[r]->() RETURN r.val, r.name").getRecords.iterator.toBag should equal(Bag(
        CypherMap("r.val" -> 42, "r.name" -> "Donald")
      ))
    }

    // TODO: Support MGC syntax
    ignore("should allow simple MGC syntax") {
      val query =
        """|CONSTRUCT {
           |  CREATE (a:A)-[r:FOO]->(b:B)
           |}
           |MATCH (a)-->(b)
           |CONSTRUCT {
           |  CREATE (a)-[:KNOWS]->(b)
           |}
           |RETURN GRAPH""".stripMargin

      val result = testGraph1.cypher(query)

      result.getRecords.toMaps shouldBe empty
      result.getGraph.schema.relationshipTypes should equal(Set("KNOWS"))
      result.getGraph.schema.labels should equal(Set("A", "B"))
      result.getGraph.schema should equal(Schema.empty
        .withNodePropertyKeys("A")()
        .withNodePropertyKeys("B")()
        .withRelationshipPropertyKeys("KNOWS")()
          .withTags(0, 1)
        .asCaps)
      result.getGraph.cypher("MATCH ()-[r]->() RETURN type(r)").getRecords.iterator.toBag should equal(Bag(
        CypherMap("type(r)" -> "KNOWS")
      ))
    }

  }

}
