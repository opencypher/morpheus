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

import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.spark.impl.CAPSGraph

import org.opencypher.okapi.ir.test.support.Bag
import org.opencypher.okapi.ir.test.support.Bag._

trait PredicateBehaviour {
  this: AcceptanceTest =>

  def predicateBehaviour(initGraph: String => CAPSGraph): Unit = {
    test("exists()") {
      val given = initGraph("CREATE ({id: 1}), ({id: 2}), ({other: 'foo'}), ()")

      val result = given.cypher("MATCH (n) WHERE exists(n.id) RETURN n.id")

      result.getRecords.toMaps should equal(Bag(
        CypherMap("n.id" -> 1),
        CypherMap("n.id" -> 2)
      ))
    }

    test("in") {
      // Given
      val given = initGraph("""CREATE (:A {val: 1}), (:A {val: 2}), (:A {val: 3})""")

      // When
      val result = given.cypher("MATCH (a:A) WHERE a.val IN [-1, 2, 5, 0] RETURN a.val")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.val" -> 2)
      ))
    }

    test("in with parameter") {
      // Given
      val given = initGraph("""CREATE (:A {val: 1}), (:A {val: 2}), (:A {val: 3})""")

      // When
      val result = given.cypher("MATCH (a:A) WHERE a.val IN $list RETURN a.val", Map("list" -> CypherList(-1, 2, 5, 0)))

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.val" -> 2)
      ))
    }

    it("evaluates or") {
      // Given
      val given = initGraph("""CREATE (:A {val: 1}), (:A {val: 2}), (:A {val: 3})""")

      // When
      val result = given.cypher("MATCH (a:A) WHERE a.val = 1 OR a.val = 2 RETURN a.val")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.val" -> 1),
        CypherMap("a.val" -> 2)
      ))
    }

    test("or on labels") {
      // Given
      val given = initGraph("""CREATE (:A {val: 1}), (:B {val: 2}), (:C {val: 3})""")

      // When
      val result = given.cypher("MATCH (a) WHERE a:A OR a:B RETURN a.val")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.val" -> 1),
        CypherMap("a.val" -> 2)
      ))
    }

    test("or with and") {
      // Given
      val given = initGraph(
        """CREATE (:A {val: 1, name: 'a'})
          |CREATE (:A {val: 2, name: 'a'})
          |CREATE (:A {val: 3, name: 'e'})
          |CREATE (:A {val: 4})
          |CREATE (:A {val: 5, name: 'e'})
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a:A) WHERE a.val = 1 OR (a.val >= 4 AND a.name = 'e') RETURN a.val, a.name")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.val" -> 1, "a.name" -> "a"),
        CypherMap("a.val" -> 5, "a.name" -> "e")
      ))
    }

    test("equality between properties") {
      // Given
      val given = initGraph(
        """ CREATE (:A {val: 1})-[:REL]->(:B {p: 2})
          |CREATE (:A {val: 2})-[:REL]->(:B {p: 1})
          |CREATE (:A {val: 100})-[:REL]->(:B {p: 100})
          |CREATE (:A {val: 1})-[:REL]->(:B)
          |CREATE (:A)-[:REL]->(:B {p: 2})
          |CREATE (:A)-[:REL]->(:B)
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a:A)-->(b:B) WHERE a.val = b.p RETURN b.p")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("b.p" -> 100)
      ))
    }

    describe("comparison operators") {
      test("less than") {

        // Given
        val given = initGraph("""CREATE (:Node {val: 4})-[:REL]->(:Node {val: 5})""")

        // When
        val result = given.cypher("MATCH (n:Node)-->(m:Node) WHERE n.val < m.val RETURN n.val")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("n.val" -> 4)
        ))
      }

      it("compares less than between different types") {

        // Given
        val given = initGraph("""CREATE (:A {val: 4})-[:REL]->(:B {val2: 'string'})""")

        // When
        val result = given.cypher("MATCH (a:A)-->(b:B) WHERE a.val < b.val2 RETURN a.val")

        // Then
        result.getRecords.collect.toBag shouldBe empty
      }

      test("less than or equal") {
        // Given
        val given = initGraph(
          """
            |CREATE (:Node {id: 1, val: 4})-[:REL]->(:Node {id: 2, val: 5})-[:REL]->(:Node {id: 3, val: 5})
            |""".stripMargin)

        // When
        val
        result = given.cypher("MATCH (n:Node)-->(m:Node) WHERE n.val <= m.val RETURN n.id, n.val")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("n.id" -> 1, "n.val" -> 4),
          CypherMap("n.id" -> 2, "n.val" -> 5)
        ))
      }

      it("compares less than or equal between different types") {

        // Given
        val given = initGraph("""CREATE (:A {val: 4})-[:REL]->(:B {val2: 'string'})""")

        // When
        val result = given.cypher("MATCH (a:A)-->(b:B) WHERE a.val <= b.val2 RETURN a.val")

        // Then
        result.getRecords.collect.toBag shouldBe empty
      }

      test("greater than") {
        // Given
        val given = initGraph("""CREATE (:Node {val: 4})-[:REL]->(:Node {val: 5})""")

        // When
        val result = given.cypher("MATCH (n:Node)<--(m:Node) WHERE n.val > m.val RETURN n.val")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("n.val" -> 5)
        ))
      }

      it("compares greater than between different types") {

        // Given
        val given = initGraph("""CREATE (:A {val: 4})-[:REL]->(:B {val2: 'string'})""")

        // When
        val result = given.cypher("MATCH (a:A)-->(b:B) WHERE a.val > b.val2 RETURN a.val")

        // Then
        result.getRecords.collect.toBag shouldBe empty
      }

      test("greater than or equal") {
        // Given
        val given = initGraph("""CREATE (:Node {id: 1, val: 4})-[:REL]->(:Node {id: 2, val: 5})-[:REL]->(:Node {id: 3, val: 5})""")

        // When
        val result = given.cypher("MATCH (n:Node)<--(m:Node) WHERE n.val >= m.val RETURN n.id, n.val")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("n.id" -> 2, "n.val" -> 5),
          CypherMap("n.id" -> 3, "n.val" -> 5)
        ))
      }

      it("compares greater than or equal between different types") {

        // Given
        val given = initGraph("""CREATE (:A {val: 4})-[:REL]->(:B {val2: 'string'})""")

        // When
        val result = given.cypher("MATCH (a:A)-->(b:B) WHERE a.val >= b.val2 RETURN a.val")

        // Then
        result.getRecords.collect.toBag shouldBe empty
      }
    }

    it("can chain range predicates") {
      val graph = initGraph("CREATE ({val: 10}), ({val: 0}), ({val: 11})")

      val query =
        """
          |MATCH (a)
          |WHERE 0 < a.val <= 10
          |RETURN a.val
        """.stripMargin

      graph.cypher(query).getRecords.collect.toBag should equal(Bag(
        CypherMap("a.val" -> 10)
      ))
    }

    test("float conversion for integer division") {
      // Given
      val given = initGraph("""CREATE (:Node {id: 1, val: 4}), (:Node {id: 2, val: 5}), (:Node {id: 3, val: 5})""")

      // When
      val result = given.cypher("MATCH (n:Node) WHERE (n.val * 1.0) / n.id >= 2.5 RETURN n.id")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("n.id" -> 1),
        CypherMap("n.id" -> 2)
      ))
    }

    test("basic pattern predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->(w {id: 3})
          |CREATE (v)-[:REL]->(w)
          |CREATE (w)-[:REL]->({id: 4})
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a)-->(b) WHERE (a)-->()-->(b) RETURN a.id, b.id")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 3L)
      ))
    }

    test("pattern predicate with var-length-expand") {
      // Given
      val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

      // When
      val result = given.cypher("MATCH (a)-->(b) WHERE (a)-[*1..3]->()-->(b) RETURN a.id, b.id")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 3L)
      ))
    }

    test("simple pattern predicate with node predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE ({id: 1})-[:REL]->({name: 'foo'})
          |CREATE ({id: 3})-[:REL]->({name: 'bar'})
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a) WHERE (a)-->({name: 'foo'}) RETURN a.id")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L)
      ))
    }

    test("simple pattern predicate with relationship predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE (v {id: 1})-[:REL {val: 'foo'}]->()-[:REL]->({id: 2})<-[:REL]-(v)
          |CREATE (w {id: 3})-[:REL {val: 'bar'}]->()-[:REL]->({id: 4})<-[:REL]-(w)
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a)-->(b) WHERE (a)-[{val: 'foo'}]->()-->(b) RETURN a.id, b.id")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 2L)
      ))
    }

    test("simple pattern predicate with node label predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE (v{id: 1})-[:REL {val: 'foo'}]->(:A)-[:REL]->({id: 2})<-[:REL]-(v)
          |CREATE (w{id: 3})-[:REL {val: 'bar'}]->(:B)-[:REL]->({id: 4})<-[:REL]-(w)
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a)-->(b) WHERE (a)-->(:A)-->(b) RETURN a.id, b.id")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 2L)
      ))
    }

    test("simple pattern predicate with relationship type predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE (v {id: 1})-[:A]->()-[:REL]->({id: 2})<-[:REL]-(v)
          |CREATE (w {id: 3})-[:B]->()-[:REL]->({id: 4})<-[:REL]-(w)
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a)-->(b) WHERE (a)-[:A]->()-->(b) RETURN a.id, b.id")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 2L)
      ))
    }

    test("inverse pattern predicate") {
      // Given
      val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

      // When
      val result = given.cypher("MATCH (a)-->(b) WHERE NOT (a)-->()-->(b) RETURN a.id, b.id")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 2L),
        CypherMap("a.id" -> 2L, "b.id" -> 3L)
      ))
    }

    test("nested pattern predicate") {
      val given = initGraph(
        """
          |CREATE ({id: 1, age: 21})
          |CREATE ({id: 2, age: 18, foo: true})
          |CREATE ({id: 3, age: 18, foo: true})-[:KNOWS]->(:Foo)
          |CREATE ({id: 4, age: 18, foo: false})-[:KNOWS]->(:Foo)
        """.stripMargin)

      val result = given.cypher(
        """
          |MATCH (a)
          |WHERE a.age > 20 OR ( (a)-[:KNOWS]->(:Foo) AND a.foo = true )
          |RETURN a.id
        """.stripMargin)

      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1),
        CypherMap("a.id" -> 3)
      ))
    }

    test("pattern predicate with derived node predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE ({id: 1, val: 0})-[:REL]->({id: 3, val: 2})
          |CREATE ({id: 2, val: 0})-[:REL]->({id: 3, val: 1})
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a) WHERE (a)-->({val: a.val + 2}) RETURN a.id")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L)
      ))
    }

    test("multiple predicate patterns") {
      // Given
      val given = initGraph("CREATE ({id: 1})-[:REL]->({id: 2, foo: true})")

      // When
      val result = given.cypher("MATCH (a) WHERE (a)-->({id: 2, foo: true}) RETURN a.id")

      // Then
      result.getRecords.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L)
      ))
    }

    describe("Inline pattern predicates") {
      test("basic pattern predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->(w {id: 3})
            |CREATE (v)-[:REL]->(w)
            |CREATE (w)-[:REL]->({id: 4})
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE (a)-->()-->(b) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 3L)
        ))
      }

      test("pattern predicate with var-length-expand") {
        // Given
        val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE (a)-[*1..3]->()-->(b) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 3L)
        ))
      }

      test("simple pattern predicate with node predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE ({id: 1})-[:REL]->({name: 'foo'})
            |CREATE ({id: 3})-[:REL]->({name: 'bar'})
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a) WHERE (a)-->({name: 'foo'}) RETURN a.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L)
        ))
      }

      test("simple pattern predicate with relationship predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE (v {id: 1})-[:REL {val: 'foo'}]->()-[:REL]->({id: 2})<-[:REL]-(v)
            |CREATE (w {id: 3})-[:REL {val: 'bar'}]->()-[:REL]->({id: 4})<-[:REL]-(w)
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE (a)-[{val: 'foo'}]->()-->(b) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 2L)
        ))
      }

      test("simple pattern predicate with node label predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE (v{id: 1})-[:REL {val: 'foo'}]->(:A)-[:REL]->({id: 2})<-[:REL]-(v)
            |CREATE (w{id: 3})-[:REL {val: 'bar'}]->(:B)-[:REL]->({id: 4})<-[:REL]-(w)
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE (a)-->(:A)-->(b) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 2L)
        ))
      }

      test("simple pattern predicate with relationship type predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE (v {id: 1})-[:A]->()-[:REL]->({id: 2})<-[:REL]-(v)
            |CREATE (w {id: 3})-[:B]->()-[:REL]->({id: 4})<-[:REL]-(w)
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE (a)-[:A]->()-->(b) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 2L)
        ))
      }

      test("inverse pattern predicate") {
        // Given
        val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE NOT (a)-->()-->(b) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 2L),
          CypherMap("a.id" -> 2L, "b.id" -> 3L)
        ))
      }

      test("nested pattern predicate") {
        val given = initGraph(
          """
            |CREATE ({id: 1, age: 21})
            |CREATE ({id: 2, age: 18, foo: true})
            |CREATE ({id: 3, age: 18, foo: true})-[:KNOWS]->(:Foo)
            |CREATE ({id: 4, age: 18, foo: false})-[:KNOWS]->(:Foo)
          """.stripMargin)

        val result = given.cypher(
          """
            |MATCH (a)
            |WHERE a.age > 20 OR ( (a)-[:KNOWS]->(:Foo) AND a.foo = true )
            |RETURN a.id
          """.stripMargin)

        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1),
          CypherMap("a.id" -> 3)
        ))
      }

      test("pattern predicate with derived node predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE ({id: 1, val: 0})-[:REL]->({id: 3, val: 2})
            |CREATE ({id: 2, val: 0})-[:REL]->({id: 3, val: 1})
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a) WHERE (a)-->({val: a.val + 2}) RETURN a.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L)
        ))
      }

      test("multiple predicate patterns") {
        // Given
        val given = initGraph("CREATE ({id: 1})-[:REL]->({id: 2, foo: true})")

        // When
        val result = given.cypher("MATCH (a) WHERE (a)-->({id: 2, foo: true}) RETURN a.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L)
        ))
      }
    }

    describe("Pattern predicates via exists") {
      test("basic pattern predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->(w {id: 3})
            |CREATE (v)-[:REL]->(w)
            |CREATE (w)-[:REL]->({id: 4})
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE EXISTS((a)-->()-->(b)) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 3L)
        ))
      }

      test("pattern predicate with var-length-expand") {
        // Given
        val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE EXISTS((a)-[*1..3]->()-->(b)) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 3L)
        ))
      }

      test("simple pattern predicate with node predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE ({id: 1})-[:REL]->({name: 'foo'})
            |CREATE ({id: 3})-[:REL]->({name: 'bar'})
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a) WHERE EXISTS((a)-->({name: 'foo'})) RETURN a.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L)
        ))
      }

      test("simple pattern predicate with relationship predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE (v {id: 1})-[:REL {val: 'foo'}]->()-[:REL]->({id: 2})<-[:REL]-(v)
            |CREATE (w {id: 3})-[:REL {val: 'bar'}]->()-[:REL]->({id: 4})<-[:REL]-(w)
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE EXISTS((a)-[{val: 'foo'}]->()-->(b)) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 2L)
        ))
      }

      test("simple pattern predicate with node label predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE (v{id: 1})-[:REL {val: 'foo'}]->(:A)-[:REL]->({id: 2})<-[:REL]-(v)
            |CREATE (w{id: 3})-[:REL {val: 'bar'}]->(:B)-[:REL]->({id: 4})<-[:REL]-(w)
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE EXISTS((a)-->(:A)-->(b)) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 2L)
        ))
      }

      test("simple pattern predicate with relationship type predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE (v {id: 1})-[:A]->()-[:REL]->({id: 2})<-[:REL]-(v)
            |CREATE (w {id: 3})-[:B]->()-[:REL]->({id: 4})<-[:REL]-(w)
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE EXISTS((a)-[:A]->()-->(b)) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 2L)
        ))
      }

      test("inverse pattern predicate") {
        // Given
        val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE NOT EXISTS((a)-->()-->(b)) RETURN a.id, b.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L, "b.id" -> 2L),
          CypherMap("a.id" -> 2L, "b.id" -> 3L)
        ))
      }

      test("nested pattern predicate") {
        val given = initGraph(
          """
            |CREATE ({id: 1, age: 21})
            |CREATE ({id: 2, age: 18, foo: true})
            |CREATE ({id: 3, age: 18, foo: true})-[:KNOWS]->(:Foo)
            |CREATE ({id: 4, age: 18, foo: false})-[:KNOWS]->(:Foo)
          """.stripMargin)

        val result = given.cypher(
          """
            |MATCH (a)
            |WHERE a.age > 20 OR ( EXISTS((a)-[:KNOWS]->(:Foo)) AND a.foo = true )
            |RETURN a.id
          """.stripMargin)

        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1),
          CypherMap("a.id" -> 3)
        ))
      }

      test("pattern predicate with derived node predicate") {
        // Given
        val given = initGraph(
          """
            |CREATE ({id: 1, val: 0})-[:REL]->({id: 3, val: 2})
            |CREATE ({id: 2, val: 0})-[:REL]->({id: 3, val: 1})
          """.stripMargin)

        // When
        val result = given.cypher("MATCH (a) WHERE EXISTS((a)-->({val: a.val + 2})) RETURN a.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L)
        ))
      }

      test("multiple predicate patterns") {
        // Given
        val given = initGraph("CREATE ({id: 1})-[:REL]->({id: 2, foo: true})")

        // When
        val result = given.cypher("MATCH (a) WHERE EXISTS((a)-->({id: 2, foo: true})) RETURN a.id")

        // Then
        result.getRecords.toMaps should equal(Bag(
          CypherMap("a.id" -> 1L)
        ))
      }
    }
  }
}
