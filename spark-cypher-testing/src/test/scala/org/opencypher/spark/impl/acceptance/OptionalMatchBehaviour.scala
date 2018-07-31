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
package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class OptionalMatchBehaviour extends CAPSTestSuite with DefaultGraphInit {

  it("optionally match") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p3:Person {name: "Eve"})
        |CREATE (p1)-[:KNOWS]->(p2)
        |CREATE (p2)-[:KNOWS]->(p3)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)
        |OPTIONAL MATCH (p1)-[e1]->(p2)-[e2]->(p3)
        |RETURN p1.name, p2.name, p3.name
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "p1.name" -> "Eve",
        "p2.name" -> null,
        "p3.name" -> null
      ),
      CypherMap(
        "p1.name" -> "Bob",
        "p2.name" -> null,
        "p3.name" -> null
      ),
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Bob",
        "p3.name" -> "Eve"
      )
    ))
  }

  it("can optionally match with predicates") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p1)-[:KNOWS]->(p2)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)
        |OPTIONAL MATCH (p1)-[e1:KNOWS]->(p2:Person)
        |RETURN p1.name, p2.name
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "p1.name" -> "Bob",
        "p2.name" -> null
      ),
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Bob"
      )
    ))
  }

  it("can optionally match already matched relationships") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p3:Person {name: "Eve"})
        |CREATE (p1)-[:KNOWS]->(p2)
        |CREATE (p2)-[:KNOWS]->(p3)
        |CREATE (p1)-[:KNOWS]->(p3)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person)
        |OPTIONAL MATCH (p1)-[e2:KNOWS]->(p3:Person)
        |RETURN p1.name, p2.name, p3.name
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Bob",
        "p3.name" -> "Eve"
      ),
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Eve",
        "p3.name" -> "Bob"
      ),
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Bob",
        "p3.name" -> "Bob"
      ),
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Eve",
        "p3.name" -> "Eve"
      ),
      CypherMap(
        "p1.name" -> "Bob",
        "p2.name" -> "Eve",
        "p3.name" -> "Eve"
      )
    ))
  }

  it("can optionally match incoming relationships") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p3:Person {name: "Frank"})
        |CREATE (p1)-[:KNOWS]->(p2)
        |CREATE (p2)-[:KNOWS]->(p3)
        |CREATE (p1)<-[:LOVES]-(p3)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person)
        |OPTIONAL MATCH (p1)<-[e2:LOVES]-(p3:Person)
        |RETURN p1.name, p2.name, p3.name
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Bob",
        "p3.name" -> "Frank"
      ),
      CypherMap(
        "p1.name" -> "Bob",
        "p2.name" -> "Frank",
        "p3.name" -> null
      )
    ))
  }

  it("can optionally match with partial matches") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p3:Person {name: "Eve"})
        |CREATE (p1)-[:KNOWS]->(p2)
        |CREATE (p2)-[:KNOWS]->(p3)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)
        |OPTIONAL MATCH (p1)-[e1:KNOWS]->(p2:Person)-[e2:KNOWS]->(p3:Person)
        |RETURN p1.name, p2.name, p3.name
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Bob",
        "p3.name" -> "Eve"
      ),
      CypherMap(
        "p1.name" -> "Bob",
        "p2.name" -> null,
        "p3.name" -> null
      ),
      CypherMap(
        "p1.name" -> "Eve",
        "p2.name" -> null,
        "p3.name" -> null
      )
    ))
  }

  it("can optionally match with duplicates") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p3:Person {name: "Eve"})
        |CREATE (p4:Person {name: "Paul"})
        |CREATE (p1)-[:KNOWS]->(p3)
        |CREATE (p2)-[:KNOWS]->(p3)
        |CREATE (p3)-[:KNOWS]->(p4)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (a:Person)-[e1:KNOWS]->(b:Person)
        |OPTIONAL MATCH (b)-[e2:KNOWS]->(c:Person)
        |RETURN b.name, c.name
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "b.name" -> "Eve",
        "c.name" -> "Paul"
      ),
      CypherMap(
        "b.name" -> "Eve",
        "c.name" -> "Paul"
      ),
      CypherMap(
        "b.name" -> "Paul",
        "c.name" -> null
      )
    ))
  }

  it("can optionally match with duplicates and cycle") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p3:Person {name: "Eve"})
        |CREATE (p4:Person {name: "Paul"})
        |CREATE (p1)-[:KNOWS]->(p3)
        |CREATE (p2)-[:KNOWS]->(p3)
        |CREATE (p3)-[:KNOWS]->(p4)
        |CREATE (p4)-[:KNOWS {foo: 42}]->(p1)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (a:Person)-[e1:KNOWS]->(b:Person)-[e2:KNOWS]->(c:Person)
        |OPTIONAL MATCH (c)-[e3:KNOWS]->(a)
        |RETURN a.name, b.name, c.name, e3.foo
      """.stripMargin)

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "a.name" -> "Alice",
        "b.name" -> "Eve",
        "c.name" -> "Paul",
        "e3.foo" -> 42
      ),
      CypherMap(
        "a.name" -> "Eve",
        "b.name" -> "Paul",
        "c.name" -> "Alice",
        "e3.foo" -> null
      ),
      CypherMap(
        "a.name" -> "Paul",
        "b.name" -> "Alice",
        "c.name" -> "Eve",
        "e3.foo" -> null
      ),
      CypherMap(
        "a.name" -> "Bob",
        "b.name" -> "Eve",
        "c.name" -> "Paul",
        "e3.foo" -> null
      )
    ))
  }

  it("can match multiple optional matches") {
    val graph = initGraph(
      """
        |CREATE (s {val: 1})
      """.stripMargin)

    val result = graph.cypher(
      """
        |MATCH (a)
        |OPTIONAL MATCH (a)-->(b:NonExistent)
        |OPTIONAL MATCH (a)-->(c:NonExistent)
        |RETURN b,c
      """.stripMargin)

    result.records.collect.toBag should equal(Bag(
      CypherMap("b" -> CypherNull, "c" -> CypherNull)
    ))
  }
}
