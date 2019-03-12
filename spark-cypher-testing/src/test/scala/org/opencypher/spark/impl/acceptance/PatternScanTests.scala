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
package org.opencypher.spark.impl.acceptance

import org.junit.runner.RunWith
import org.opencypher.okapi.api.graph.{NodeRelPattern, TripletPattern}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PatternScanTests extends CAPSTestSuite with ScanGraphInit {

  it("resolves simple expands") {
    val pattern = NodeRelPattern(CTNode("Person"), CTRelationship("KNOWS"))

    val g = initGraph(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |
        |CREATE (a)-[:KNOWS]->(b)
      """.stripMargin,
      Seq(pattern)
    )

    val res = g.cypher(
      """
        |MATCH (a:Person)-[:KNOWS]->(b:Person)
        |RETURN a.name, b.name
      """.stripMargin)

    res.records.toMaps should equal(Bag(
      CypherMap("a.name" -> "Alice", "b.name" -> "Bob")
    ))
  }

  it("can combine multiple pattern scans") {
    val pattern1 = NodeRelPattern(CTNode("Person"), CTRelationship("KNOWS"))
    val pattern2 = TripletPattern(CTNode("Person"), CTRelationship("LOVES"), CTNode("Person"))

    val g = initGraph(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |CREATE (c:Person {name: "Carol"})
        |
        |CREATE (a)-[:KNOWS]->(b)
        |CREATE (a)-[:LOVES]->(c)
      """.stripMargin,
      Seq(pattern1, pattern2)
    )

    val res = g.cypher(
      """
        |MATCH (a:Person)-[:KNOWS]->(b:Person),
        |      (a)-[:LOVES]->(c:Person)
        |RETURN a.name, b.name, c.name
      """.stripMargin)

    res.records.toMaps should equal(Bag(
      CypherMap("a.name" -> "Alice", "b.name" -> "Bob", "c.name" -> "Carol")
    ))
  }

  it("combines multiple pattern scans") {
    val pattern1 = NodeRelPattern(CTNode("Person"), CTRelationship("KNOWS"))
    val pattern2 = NodeRelPattern(CTNode("Person", "Employee"), CTRelationship("KNOWS"))

    val g = initGraph(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |CREATE (c:Person:Employee {name: "Garfield"})
        |
        |CREATE (a)-[:KNOWS]->(b)
        |CREATE (c)-[:KNOWS]->(b)
      """.stripMargin,
      Seq(pattern1, pattern2)
    )

    val res = g.cypher(
      """
        |MATCH (a:Person)-[:KNOWS]->(b:Person)
        |RETURN a.name, b.name
      """.stripMargin)

    res.records.toMaps should equal(Bag(
      CypherMap("a.name" -> "Alice", "b.name" -> "Bob"),
      CypherMap("a.name" -> "Garfield", "b.name" -> "Bob")
    ))
  }

  it("works if node rel scans do not cover all node label combinations") {
    val pattern1 = NodeRelPattern(CTNode("Person"), CTRelationship("KNOWS"))

    val g = initGraph(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |CREATE (c:Person:Employee {name: "Garfield"})
        |
        |CREATE (a)-[:KNOWS]->(b)
        |CREATE (c)-[:KNOWS]->(b)
      """.stripMargin,
      Seq(pattern1)
    )

    val res = g.cypher(
      """
        |MATCH (a:Person)-[:KNOWS]->(b:Person)
        |RETURN a.name, b.name
      """.stripMargin)

    res.records.toMaps should equal(Bag(
      CypherMap("a.name" -> "Alice", "b.name" -> "Bob"),
      CypherMap("a.name" -> "Garfield", "b.name" -> "Bob")
    ))
  }

  it("works if node rel scans do not cover all rel type combinations") {
    val pattern1 = NodeRelPattern(CTNode("Person"), CTRelationship("KNOWS"))

    val g = initGraph(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |
        |CREATE (a)-[:KNOWS]->(b)
        |CREATE (a)-[:LOVES]->(b)
      """.stripMargin,
      Seq(pattern1)
    )

    val res = g.cypher(
      """
        |MATCH (a:Person)-[:KNOWS|LOVES]->(b:Person)
        |RETURN a.name, b.name
      """.stripMargin)

    res.records.toMaps should equal(Bag(
      CypherMap("a.name" -> "Alice", "b.name" -> "Bob"),
      CypherMap("a.name" -> "Alice", "b.name" -> "Bob")
    ))
  }

  it("works if triplet scans do not cover all source node labels") {
    val pattern = TripletPattern(CTNode("Person"), CTRelationship("KNOWS"), CTNode("Person"))

    val g = initGraph(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |CREATE (c:Animal {name: "Garfield"})
        |
        |CREATE (a)-[:KNOWS]->(b)
        |CREATE (c)-[:KNOWS]->(b)
      """.stripMargin,
      Seq(pattern)
    )

    val res = g.cypher(
      """
        |MATCH (a)-[:KNOWS]->(b:Person)
        |RETURN a.name, b.name
      """.stripMargin)

    res.records.toMaps should equal(Bag(
      CypherMap("a.name" -> "Alice", "b.name" -> "Bob"),
      CypherMap("a.name" -> "Garfield", "b.name" -> "Bob")
    ))
  }

  it("works if triplet scans do not cover all target node labels") {
    val pattern = TripletPattern(CTNode("Person"), CTRelationship("KNOWS"), CTNode("Person"))

    val g = initGraph(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |CREATE (c:Animal {name: "Garfield"})
        |
        |CREATE (a)-[:KNOWS]->(b)
        |CREATE (a)-[:KNOWS]->(c)
      """.stripMargin,
      Seq(pattern)
    )

    val res = g.cypher(
      """
        |MATCH (a)-[:KNOWS]->(b)
        |RETURN a.name, b.name
      """.stripMargin)

    res.records.toMaps should equal(Bag(
      CypherMap("a.name" -> "Alice", "b.name" -> "Bob"),
      CypherMap("a.name" -> "Alice", "b.name" -> "Garfield")
    ))
  }

  it("works if they do not cover all rel types") {
    val pattern = TripletPattern(CTNode("Person"), CTRelationship("KNOWS"), CTNode("Person"))

    val g = initGraph(
      """
        |CREATE (a:Person {name: "Alice"})
        |CREATE (b:Person {name: "Bob"})
        |
        |CREATE (a)-[:KNOWS]->(b)
        |CREATE (a)-[:LOVES]->(b)
      """.stripMargin,
      Seq(pattern)
    )

    val res = g.cypher(
      """
        |MATCH (a)-[:KNOWS|LOVES]->(b)
        |RETURN a.name, b.name
      """.stripMargin)

    res.records.toMaps should equal(Bag(
      CypherMap("a.name" -> "Alice", "b.name" -> "Bob"),
      CypherMap("a.name" -> "Alice", "b.name" -> "Bob")
    ))
  }
}
