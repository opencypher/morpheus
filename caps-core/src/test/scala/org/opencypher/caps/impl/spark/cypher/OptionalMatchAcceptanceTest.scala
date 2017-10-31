/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.spark.cypher

import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.test.CAPSTestSuite

import scala.collection.Bag

class OptionalMatchAcceptanceTest extends CAPSTestSuite {

  test("optional match") {
    // Given
    val given = TestGraph(
      """
        |(p1:Person {name: "Alice"}),
        |(p2:Person {name: "Bob"}),
        |(p3:Person {name: "Eve"}),
        |(p1)-[:KNOWS]->(p2),
        |(p2)-[:KNOWS]->(p3),
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
    result.graphs shouldBe empty
  }

  test("optional match with predicates") {
    // Given
    val given = TestGraph(
      """
        |(p1:Person {name: "Alice"}),
        |(p2:Person {name: "Bob"}),
        |(p1)-[:KNOWS]->(p2),
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
    result.graphs shouldBe empty
  }

  test("optional match with partial matches") {
    // Given
    val given = TestGraph(
      """
        |(p1:Person {name: "Alice"}),
        |(p2:Person {name: "Bob"}),
        |(p3:Person {name: "Eve"}),
        |(p1)-[:KNOWS]->(p2),
        |(p2)-[:KNOWS]->(p3)
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
    result.graphs shouldBe empty
  }

  test("optional match with duplicates") {
    // Given
    val given = TestGraph(
      """
        |(p1:Person {name: "Alice"}),
        |(p2:Person {name: "Bob"}),
        |(p3:Person {name: "Eve"}),
        |(p4:Person {name: "Paul"}),
        |(p1)-[:KNOWS]->(p3),
        |(p2)-[:KNOWS]->(p3),
        |(p3)-[:KNOWS]->(p4),
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
    result.graphs shouldBe empty
  }

  test("optional match with duplicates and cycle") {
    // Given
    val given = TestGraph(
      """
        |(p1:Person {name: "Alice"}),
        |(p2:Person {name: "Bob"}),
        |(p3:Person {name: "Eve"}),
        |(p4:Person {name: "Paul"}),
        |(p1)-[:KNOWS]->(p3),
        |(p2)-[:KNOWS]->(p3),
        |(p3)-[:KNOWS]->(p4),
        |(p4)-[:KNOWS {foo: 42L}]->(p1)
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
    result.graphs shouldBe empty
  }
}
