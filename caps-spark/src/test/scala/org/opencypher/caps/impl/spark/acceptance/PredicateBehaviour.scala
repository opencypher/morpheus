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
package org.opencypher.caps.impl.spark.acceptance

import org.opencypher.caps.api.value.{CAPSList, CAPSMap}
import org.opencypher.caps.impl.spark.CAPSGraph

import scala.collection.immutable.Bag

trait PredicateBehaviour {
  this: AcceptanceTest =>

  def predicateBehaviour(initGraph: String => CAPSGraph): Unit = {
    test("exists()") {
      val given = initGraph("CREATE ({id: 1}), ({id: 2}), ({other: 'foo'}), ()")

      val result = given.cypher("MATCH (n) WHERE exists(n.id) RETURN n.id")

      result.records.toMaps should equal(Bag(
        CAPSMap("n.id" -> 1),
        CAPSMap("n.id" -> 2)
      ))
    }

    test("in") {
      // Given
      val given = initGraph("""CREATE (:A {val: 1}), (:A {val: 2}), (:A {val: 3})""")

      // When
      val result = given.cypher("MATCH (a:A) WHERE a.val IN [-1, 2, 5, 0] RETURN a.val")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("a.val" -> 2)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("in with parameter") {
      // Given
      val given = initGraph("""CREATE (:A {val: 1}), (:A {val: 2}), (:A {val: 3})""")

      // When
      val result = given.cypher("MATCH (a:A) WHERE a.val IN $list RETURN a.val", Map("list" -> CAPSList(Seq(-1, 2, 5, 0))))

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("a.val" -> 2)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("or") {
      // Given
      val given = initGraph("""CREATE (:A {val: 1}), (:A {val: 2}), (:A {val: 3})""")

      // When
      val result = given.cypher("MATCH (a:A) WHERE a.val = 1 OR a.val = 2 RETURN a.val")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("a.val" -> 1),
        CAPSMap("a.val" -> 2)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("or on labels") {
      // Given
      val given = initGraph("""CREATE (:A {val: 1}), (:B {val: 2}), (:C {val: 3})""")

      // When
      val result = given.cypher("MATCH (a) WHERE a:A OR a:B RETURN a.val")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("a.val" -> 1),
        CAPSMap("a.val" -> 2)
      ))

      // And
      result.graphs shouldBe empty
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
      result.records.toMaps should equal(Bag(
        CAPSMap("a.val" -> 1, "a.name" -> "a"),
        CAPSMap("a.val" -> 5, "a.name" -> "e")
      ))

      // And
      result.graphs shouldBe empty
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
      result.records.toMaps should equal(Bag(
        CAPSMap("b.p" -> 100)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("less than") {

      // Given
      val given = initGraph("""CREATE (:Node {val: 4})-[:REL]->(:Node {val: 5})""")

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) WHERE n.val < m.val RETURN n.val")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("n.val" -> 4)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("less than or equal") {
      // Given
      val given = initGraph(
        """
          |CREATE (:Node {id: 1, val: 4})-[:REL]->(:Node {id: 2, val: 5})-[:REL]->(:Node {id: 3, val: 5})
          |""".stripMargin)

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) WHERE n.val <= m.val RETURN n.id, n.val")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("n.id" -> 1, "n.val" -> 4),
        CAPSMap("n.id" -> 2, "n.val" -> 5)
      ))
      // And
      result.graphs shouldBe empty
    }

    test("greater than") {
      // Given
      val given = initGraph("""CREATE (:Node {val: 4})-[:REL]->(:Node {val: 5})""")

      // When
      val result = given.cypher("MATCH (n:Node)<--(m:Node) WHERE n.val > m.val RETURN n.val")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("n.val" -> 5)
      ))

      // And
      result.graphs shouldBe empty
    }

    test("greater than or equal") {
      // Given
      val given = initGraph("""CREATE (:Node {id: 1, val: 4})-[:REL]->(:Node {id: 2, val: 5})-[:REL]->(:Node {id: 3, val: 5})""")

      // When
      val result = given.cypher("MATCH (n:Node)<--(m:Node) WHERE n.val >= m.val RETURN n.id, n.val")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("n.id" -> 2, "n.val" -> 5),
        CAPSMap("n.id" -> 3, "n.val" -> 5)
      ))

      // And
      result.graphs shouldBe empty
    }

    it("can chain range predicates") {
      val graph = initGraph("CREATE ({val: 10}), ({val: 0}), ({val: 11})")

      val query =
        """
          |MATCH (a)
          |WHERE 0 < a.val <= 10
          |RETURN a.val
        """.stripMargin

      graph.cypher(query).records.iterator.toBag should equal(Bag(
        CAPSMap("a.val" -> 10)
      ))
    }

    test("float conversion for integer division") {
      // Given
      val given = initGraph("""CREATE (:Node {id: 1, val: 4}), (:Node {id: 2, val: 5}), (:Node {id: 3, val: 5})""")

      // When
      val result = given.cypher("MATCH (n:Node) WHERE (n.val * 1.0) / n.id >= 2.5 RETURN n.id")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("n.id" -> 1),
        CAPSMap("n.id" -> 2)
      ))

      // And
      result.graphs shouldBe empty
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
      result.records.toMaps should equal(Bag(
        CAPSMap("a.id" -> 1L, "b.id" -> 3L)
      ))
    }

    test("pattern predicate with var-length-expand") {
      // Given
      val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

      // When
      val result = given.cypher("MATCH (a)-->(b) WHERE (a)-[*1..3]->()-->(b) RETURN a.id, b.id")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("a.id" -> 1L, "b.id" -> 3L)
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
      result.records.toMaps should equal(Bag(
        CAPSMap("a.id" -> 1L)
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
      result.records.toMaps should equal(Bag(
        CAPSMap("a.id" -> 1L, "b.id" -> 2L)
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
      result.records.toMaps should equal(Bag(
        CAPSMap("a.id" -> 1L, "b.id" -> 2L)
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
      result.records.toMaps should equal(Bag(
        CAPSMap("a.id" -> 1L, "b.id" -> 2L)
      ))
    }

    test("inverse pattern predicate") {
      // Given
      val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

      // When
      val result = given.cypher("MATCH (a)-->(b) WHERE NOT (a)-->()-->(b) RETURN a.id, b.id")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("a.id" -> 1L, "b.id" -> 2L),
        CAPSMap("a.id" -> 2L, "b.id" -> 3L)
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

      result.records.toMaps should equal(Bag(
        CAPSMap("a.id" -> 1),
        CAPSMap("a.id" -> 3)
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
      result.records.toMaps should equal(Bag(
        CAPSMap("a.id" -> 1L)
      ))
    }

    test("multiple predicate patterns") {
      // Given
      val given = initGraph("CREATE ({id: 1})-[:REL]->({id: 2, foo: true})")

      // When
      val result = given.cypher("MATCH (a) WHERE (a)-->({id: 2, foo: true}) RETURN a.id")

      // Then
      result.records.toMaps should equal(Bag(
        CAPSMap("a.id" -> 1L)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 3L)
        ))
      }

      test("pattern predicate with var-length-expand") {
        // Given
        val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE (a)-[*1..3]->()-->(b) RETURN a.id, b.id")

        // Then
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 3L)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 2L)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 2L)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 2L)
        ))
      }

      test("inverse pattern predicate") {
        // Given
        val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE NOT (a)-->()-->(b) RETURN a.id, b.id")

        // Then
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 2L),
          CAPSMap("a.id" -> 2L, "b.id" -> 3L)
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

        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1),
          CAPSMap("a.id" -> 3)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L)
        ))
      }

      test("multiple predicate patterns") {
        // Given
        val given = initGraph("CREATE ({id: 1})-[:REL]->({id: 2, foo: true})")

        // When
        val result = given.cypher("MATCH (a) WHERE (a)-->({id: 2, foo: true}) RETURN a.id")

        // Then
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 3L)
        ))
      }

      test("pattern predicate with var-length-expand") {
        // Given
        val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE EXISTS((a)-[*1..3]->()-->(b)) RETURN a.id, b.id")

        // Then
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 3L)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 2L)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 2L)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 2L)
        ))
      }

      test("inverse pattern predicate") {
        // Given
        val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

        // When
        val result = given.cypher("MATCH (a)-->(b) WHERE NOT EXISTS((a)-->()-->(b)) RETURN a.id, b.id")

        // Then
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L, "b.id" -> 2L),
          CAPSMap("a.id" -> 2L, "b.id" -> 3L)
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

        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1),
          CAPSMap("a.id" -> 3)
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
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L)
        ))
      }

      test("multiple predicate patterns") {
        // Given
        val given = initGraph("CREATE ({id: 1})-[:REL]->({id: 2, foo: true})")

        // When
        val result = given.cypher("MATCH (a) WHERE EXISTS((a)-->({id: 2, foo: true})) RETURN a.id")

        // Then
        result.records.toMaps should equal(Bag(
          CAPSMap("a.id" -> 1L)
        ))
      }
    }
  }
}
