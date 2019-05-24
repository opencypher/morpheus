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
package org.opencypher.morpheus.impl.acceptance

import claimant.Claim
import org.opencypher.morpheus.impl.SparkSQLMappingException
import org.opencypher.morpheus.testing.MorpheusTestSuite
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.Format.defaultValueFormatter
import org.opencypher.okapi.api.value.CypherValue.{CypherFloat, CypherInteger, CypherList, CypherMap}
import org.opencypher.okapi.api.value.GenCypherValue._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.temporal.Duration
import org.opencypher.okapi.ir.impl.exception.ParsingException
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.scalacheck.Prop
import org.scalatestplus.scalacheck.Checkers

class ExpressionTests extends MorpheusTestSuite with ScanGraphInit with Checkers {

  describe("list slice") {

    it("slice") {
      val result = morpheus.cypher("RETURN ['a', 'b', 'c', 'd'][0..3] as r")
      result.records.toMaps should equal(Bag(
        CypherMap("r" -> CypherList("a", "b", "c"))
      ))
    }

    it("slice without from") {
      val result = morpheus.cypher("RETURN ['a', 'b', 'c', 'd'][..3] as r")
      result.records.toMaps should equal(Bag(
        CypherMap("r" -> CypherList("a", "b", "c"))
      ))
    }

    it("slice without to") {
      val result = morpheus.cypher("RETURN ['a', 'b', 'c', 'd'][0..] as r")
      result.records.toMaps should equal(Bag(
        CypherMap("r" -> CypherList("a", "b", "c", "d"))
      ))
    }

  }

  describe("CASE") {
    it("should evaluate a generic CASE expression with default") {
      // Given
      val given =
        initGraph(
          """
            |CREATE (:Person {val: "foo"})
            |CREATE (:Person {val: "bar"})
            |CREATE (:Person {val: "baz"})
          """.stripMargin)

      // When
      val result = given.cypher(
        """MATCH (n)
          |RETURN
          | n.val,
          | CASE
          |   WHEN n.val = 'foo' THEN 1
          |   WHEN n.val = 'bar' THEN 2
          |   ELSE 3
          | END AS result
        """.stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val" -> "foo", "result" -> 1),
        CypherMap("n.val" -> "bar", "result" -> 2),
        CypherMap("n.val" -> "baz", "result" -> 3))
      )
    }

    it("should evaluate a simple equality CASE expression") {
      // Given
      val given =
        initGraph(
          """
            |CREATE (:Person {val: "foo"})
            |CREATE (:Person {val: "bar"})
            |CREATE (:Person {val: "baz"})
          """.stripMargin)

      // When
      val result = given.cypher(
        """MATCH (n)
          |RETURN
          | n.val,
          | CASE n.val
          |   WHEN 'foo' THEN 1
          |   WHEN 'bar' THEN 2
          |   ELSE 3
          | END AS result
        """.stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val" -> "foo", "result" -> 1),
        CypherMap("n.val" -> "bar", "result" -> 2),
        CypherMap("n.val" -> "baz", "result" -> 3))
      )
    }

    it("should evaluate an inner CASE expression with default") {
      // Given
      val given =
        initGraph(
          """
            |CREATE (:Person {val: "foo", amount: 42 })
            |CREATE (:Person {val: "bar", amount: 23 })
            |CREATE (:Person {val: "baz", amount: 84 })
          """.stripMargin)

      // When
      val result = given.cypher(
        """MATCH (n)
          |RETURN
          | n.val,
          | sum(CASE n.val
          |   WHEN 'foo' THEN n.amount
          |   WHEN 'bar' THEN 1984
          |   ELSE 0
          | END) AS result
        """.stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("n.val" -> "foo", "result" -> 42),
        CypherMap("n.val" -> "bar", "result" -> 1984),
        CypherMap("n.val" -> "baz", "result" -> 0))
      )
    }

  }

  describe("properties") {
    it("handles property expression on unknown label") {
      // Given
      val given = initGraph(
        """
          |CREATE (p:Person {firstName: "Alice", lastName: "Foo"})
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (a:Animal)
          |RETURN a.name
        """.stripMargin
      )

      // Then
      result.records.toMaps shouldBe empty
    }

    it("handles unknown properties") {
      // Given
      val given = initGraph(
        """
          |CREATE (p:Person {firstName: "Alice", lastName: "Foo"})
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (a:Person)
          |RETURN a.firstName, a.age
        """.stripMargin)

      // Then
      result.records.toMaps should equal(Bag(CypherMap("a.age" -> null, "a.firstName" -> "Alice")))
    }

    it("equality between properties") {
      // Given
      val given = initGraph(
        """
          |CREATE (:A {val: 1})-[:REL]->(:B {p: 2})
          |CREATE (:A {val: 2})-[:REL]->(:B {p: 1})
          |CREATE (:A {val: 100})-[:REL]->(:B {p: 100})
          |CREATE (:A {val: 1})-[:REL]->(:B)
          |CREATE (:A)-[:REL]->(:B {p: 2})
          |CREATE (:A)-[:REL]->(:B)
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a:A)-->(b:B) RETURN a.val = b.p AS eq")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("eq" -> false),
        CypherMap("eq" -> false),
        CypherMap("eq" -> true),
        CypherMap("eq" -> null),
        CypherMap("eq" -> null),
        CypherMap("eq" -> null)
      ))
    }

    it("filter rels on property regular expression") {
      // Given
      val given = initGraph(
        """CREATE (rachel:Person:Actor {name: 'Rachel Kempson', birthyear: 1910})
          |CREATE (michael:Person:Actor {name: 'Michael Redgrave', birthyear: 1908})
          |CREATE (corin:Person:Actor {name: 'Corin Redgrave', birthyear: 1939})
          |CREATE (liam:Person:Actor {name: 'Liam Neeson', birthyear: 1952})
          |CREATE (richard:Person:Actor {name: 'Richard Harris', birthyear: 1930})
          |CREATE (dennis:Person:Actor {name: 'Dennis Quaid', birthyear: 1954})
          |CREATE (lindsay:Person:Actor {name: 'Lindsay Lohan', birthyear: 1986})
          |CREATE (jemma:Person:Actor {name: 'Jemma Redgrave', birthyear: 1965})
          |
          |CREATE (mrchips:Film {title: 'Goodbye, Mr. Chips'})
          |CREATE (batmanbegins:Film {title: 'Batman Begins'})
          |CREATE (harrypotter:Film {title: 'Harry Potter and the Sorcerer\'s Stone'})
          |CREATE (parent:Film {title: 'The Parent Trap'})
          |CREATE (camelot:Film {title: 'Camelot'})
          |
          |CREATE (michael)-[:ACTED_IN {charactername: 'The Headmaster'}]->(mrchips),
          |       (richard)-[:ACTED_IN {charactername: 'King Arthur'}]->(camelot),
          |       (richard)-[:ACTED_IN {charactername: 'Albus Dumbledore'}]->(harrypotter),
          |       (dennis)-[:ACTED_IN {charactername: 'Nick Parker'}]->(parent),
          |       (lindsay)-[:ACTED_IN {charactername: 'Halle/Annie'}]->(parent),
          |       (liam)-[:ACTED_IN {charactername: 'Henri Ducard'}]->(batmanbegins)
        """.stripMargin)

      // When
      val query =
        """MATCH (a:Actor)-[r:ACTED_IN]->() WHERE r.charactername =~ '(\\w+\\s*)*Du\\w+' RETURN r.charactername"""
      val result = given.cypher(query)

      // Then
      val records = result.records.collect
      records.toBag should equal(Bag(CypherMap("r.charactername" -> "Henri Ducard"),
        CypherMap("r.charactername" -> "Albus Dumbledore")))
    }

    it("filter nodes on property regular expression") {
      // Given
      val given = initGraph(
        """CREATE (rachel:Person:Actor {name: 'Rachel Kempson', birthyear: 1910})
          |CREATE (michael:Person:Actor {name: 'Michael Redgrave', birthyear: 1908})
          |CREATE (corin:Person:Actor {name: 'Corin Redgrave', birthyear: 1939})
          |CREATE (liam:Person:Actor {name: 'Liam Neeson', birthyear: 1952})
          |CREATE (richard:Person:Actor {name: 'Richard Harris', birthyear: 1930})
          |CREATE (dennis:Person:Actor {name: 'Dennis Quaid', birthyear: 1954})
          |CREATE (lindsay:Person:Actor {name: 'Lindsay Lohan', birthyear: 1986})
          |CREATE (jemma:Person:Actor {name: 'Jemma Redgrave', birthyear: 1965})
          |
          |CREATE (mrchips:Film {title: 'Goodbye, Mr. Chips'})
          |CREATE (batmanbegins:Film {title: 'Batman Begins'})
          |CREATE (harrypotter:Film {title: 'Harry Potter and the Sorcerer\'s Stone'})
          |CREATE (parent:Film {title: 'The Parent Trap'})
          |CREATE (camelot:Film {title: 'Camelot'})
          |
          |CREATE (michael)-[:ACTED_IN {charactername: 'The Headmaster'}]->(mrchips),
          |       (richard)-[:ACTED_IN {charactername: 'King Arthur'}]->(camelot),
          |       (richard)-[:ACTED_IN {charactername: 'Albus Dumbledore'}]->(harrypotter),
          |       (dennis)-[:ACTED_IN {charactername: 'Nick Parker'}]->(parent),
          |       (lindsay)-[:ACTED_IN {charactername: 'Halle/Annie'}]->(parent),
          |       (liam)-[:ACTED_IN {charactername: 'Henri Ducard'}]->(batmanbegins)
        """.stripMargin)

      // When
      val query =
        """MATCH (p:Person) WHERE p.name =~ '\\w+ Redgrave' RETURN p.name"""
      val result = given.cypher(query)

      // Then
      val records = result.records.collect
      records.toBag should equal(Bag(CypherMap("p.name" -> "Michael Redgrave"),
        CypherMap("p.name" -> "Corin Redgrave"),
        CypherMap("p.name" -> "Jemma Redgrave")))

    }

    it("supports simple property expression") {
      // Given
      val given = initGraph("CREATE (:Person {name: 'Mats'})-[:REL]->(:Person {name: 'Martin'})")

      // When
      val result = given.cypher("MATCH (p:Person) RETURN p.name")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("p.name" -> "Mats"),
        CypherMap("p.name" -> "Martin")
      ))
    }

    it("supports simple property expression on relationship") {
      // Given
      val given = initGraph("CREATE (:Person {name: 'Mats'})-[:KNOWS {since: 2017}]->(:Person {name: 'Martin'})")

      // When
      val result = given.cypher("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("r.since" -> 2017)
      ))
    }

    it("reports error message when property has ANY type") {
      // Given
      val given = initGraph("""CREATE (:A {val: 'foo'}), (:B {val: 1}), (:C)""")

      // When
      val result = given.cypher("MATCH (a) RETURN a.val")

      // Then
      val e = the[IllegalArgumentException] thrownBy result.records.size
      e.getMessage should include("Equal column data types")
    }
  }

  test("less than") {

    // Given
    val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5})-[:REL]->({val: 5})-[:REL]->({val: 2})-[:REL]->()")

    // When
    val result = given.cypher("MATCH (n)-->(m) RETURN n.val < m.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val < m.val" -> true),
      CypherMap("n.val < m.val" -> false),
      CypherMap("n.val < m.val" -> false),
      CypherMap("n.val < m.val" -> null)
    ))
  }

  test("less than or equal") {
    // Given
    val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5})-[:REL]->({val: 5})-[:REL]->({val: 2})-[:REL]->()")

    // When
    val result = given.cypher("MATCH (n)-->(m) RETURN n.val <= m.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val <= m.val" -> true),
      CypherMap("n.val <= m.val" -> true),
      CypherMap("n.val <= m.val" -> false),
      CypherMap("n.val <= m.val" -> null)
    ))
  }

  test("greater than") {
    // Given
    val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5})-[:REL]->({val: 5})-[:REL]->({val: 2})-[:REL]->()")

    // When
    val result = given.cypher("MATCH (n)-->(m) RETURN n.val > m.val AS gt")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("gt" -> false),
      CypherMap("gt" -> false),
      CypherMap("gt" -> true),
      CypherMap("gt" -> null)
    ))
  }

  test("greater than or equal") {
    // Given
    val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5})-[:REL]->({val: 5})-[:REL]->({val: 2})-[:REL]->()")

    // When
    val result = given.cypher("MATCH (n)-->(m) RETURN n.val >= m.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val >= m.val" -> false),
      CypherMap("n.val >= m.val" -> true),
      CypherMap("n.val >= m.val" -> true),
      CypherMap("n.val >= m.val" -> null)
    ))
  }

  it("supports integer addition") {
    check(Prop.forAll(integer, integer) { (i1: CypherInteger, i2: CypherInteger) =>
      val query = s"RETURN ${i1.toCypherString} + ${i2.toCypherString} AS result"
      if (BigInt(i1.unwrap) + BigInt(i2.unwrap) != BigInt(i1.unwrap + i2.unwrap)) {
        // Long over-/underflow
        val e = the[ParsingException] thrownBy morpheus.cypher(query).records.toMaps
        Claim(e.getMessage.contains("SemanticError") && e.getMessage.contains("cannot be represented as an integer"))
      } else {
        val result = morpheus.cypher(query).records.toMaps
        val expected = Bag(CypherMap("result" -> (i1.unwrap + i2.unwrap)))
        Claim(result == expected)
      }
    }, minSuccessful(100))
  }

  it("supports float addition") {
    check(Prop.forAll(float, float) { (f1: CypherFloat, f2: CypherFloat) =>
      val query = s"RETURN ${f1.toCypherString} + ${f2.toCypherString} AS result"
      val result = morpheus.cypher(query).records.toMaps
      val expected = Bag(CypherMap("result" -> (f1.unwrap + f2.unwrap)))
      Claim(result == expected)
    }, minSuccessful(100))
  }

  it("supports addition after matching a pattern") {
    // Given
    val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5, other: 3})-[:REL]->()")

    // When
    val result = given.cypher("MATCH (n)-->(m) RETURN m.other + m.val + n.val AS res")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 12),
      CypherMap("res" -> null)
    ))
  }

  test("subtraction with name") {
    // Given
    val given = initGraph("CREATE ({val: 4})-[:REL]->({val: 5, other: 3})-[:REL]->()")

    // When
    val result = given.cypher("MATCH (n)-->(m) RETURN m.val - n.val - m.other AS res")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("res" -> -2),
      CypherMap("res" -> null)
    ))
  }

  test("subtraction without name") {
    // Given
    val given = initGraph("CREATE (:Node {val: 4})-[:REL]->(:Node {val: 5})")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN m.val - n.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("m.val - n.val" -> 1)
    ))
  }

  test("multiplication with integer") {
    // Given
    val given = initGraph("CREATE (:Node {val: 9})-[:REL]->(:Node {val: 2})-[:REL]->(:Node {val: 3})")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val * m.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val * m.val" -> 18),
      CypherMap("n.val * m.val" -> 6)
    ))

  }

  test("multiplication with float") {
    // Given
    val given = initGraph("CREATE (:Node {val: 4.5D})-[:REL]->(:Node {val: 2.5D})")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val * m.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val * m.val" -> 11.25)
    ))

  }

  test("multiplication with integer and float") {
    // Given
    val given = initGraph("CREATE (:Node {val: 9})-[:REL]->(:Node {val2: 2.5D})")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val * m.val2")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val * m.val2" -> 22.5)
    ))

  }

  test("division with no remainder") {
    // Given
    val given = initGraph("CREATE (:Node {val: 9})-[:REL]->(:Node {val: 3})-[:REL]->(:Node {val: 2})")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val / m.val")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val / m.val" -> 3),
      CypherMap("n.val / m.val" -> 1)
    ))

  }

  test("division integer and float and null") {
    // Given
    val given = initGraph("CREATE (:Node {val: 9})-[:REL]->(:Node {val2: 4.5D})-[:REL]->(:Node)")

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN n.val / m.val2")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("n.val / m.val2" -> 2.0),
      CypherMap("n.val / m.val2" -> null)
    ))

  }

  test("division float and literal") {
    // Given
    val given = initGraph("CREATE (:Node {val: 4.5})")

    // When
    val result = given.cypher("MATCH (n:Node) RETURN n.val / 0.5 AS res")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("res" -> 9.0)
    ))

  }


  it("equality") {
    // Given
    val given = initGraph(
      """
        |CREATE (:Node {val: 4})-[:REL]->(:Node {val: 5})
        |CREATE (:Node {val: 4})-[:REL]->(:Node {val: 4})
        |CREATE (:Node)-[:REL]->(:Node {val: 5})
      """.stripMargin)

    // When
    val result = given.cypher("MATCH (n:Node)-->(m:Node) RETURN m.val = n.val AS res")

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap("res" -> false),
      CypherMap("res" -> true),
      CypherMap("res" -> null)
    ))
  }

  describe("EXISTS with pattern") {
    it("evaluates basic exists pattern") {
      // Given
      val given = initGraph(
        """
          |CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->(w {id: 3})
          |CREATE (v)-[:REL]->(w)
          |CREATE (w)-[:REL]->({id: 4})
        """.stripMargin)

      // When
      val result = given.cypher("MATCH (a)-->(b) WITH a, b, EXISTS((a)-->()-->(b)) as con RETURN a.id, b.id, con")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 3L, "con" -> true),
        CypherMap("a.id" -> 1L, "b.id" -> 2L, "con" -> false),
        CypherMap("a.id" -> 2L, "b.id" -> 3L, "con" -> false),
        CypherMap("a.id" -> 3L, "b.id" -> 4L, "con" -> false)
      ))
    }

    it("evaluates exists pattern with var-length-expand") {
      // Given
      val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})-[:REL]->({id: 3})<-[:REL]-(v)")

      // When
      val result = given.cypher(
        """
          |MATCH (a)-->(b)
          |WITH a, b, EXISTS((a)-[*1..3]->()-->(b)) as con
          |RETURN a.id, b.id, con""".stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 2L, "con" -> false),
        CypherMap("a.id" -> 1L, "b.id" -> 3L, "con" -> true),
        CypherMap("a.id" -> 2L, "b.id" -> 3L, "con" -> false)
      ))
    }

    it("can evaluate simple exists pattern with node predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE ({id: 1})-[:REL]->({id: 2, name: 'foo'})
          |CREATE ({id: 3})-[:REL]->({id: 4, name: 'bar'})
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (a)
          |WITH a, EXISTS((a)-->({name: 'foo'})) AS con
          |RETURN a.id, con""".stripMargin)


      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "con" -> true),
        CypherMap("a.id" -> 2L, "con" -> false),
        CypherMap("a.id" -> 3L, "con" -> false),
        CypherMap("a.id" -> 4L, "con" -> false)
      ))
    }

    it("can evaluate simple exists pattern with relationship predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE (v {id: 1})-[:REL {val: 'foo'}]->({id: 2})<-[:REL]-(v)
          |CREATE (w {id: 3})-[:REL {val: 'bar'}]->({id: 4})<-[:REL]-(w)
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (a)-->(b)
          |WITH DISTINCT a, b
          |WITH a, b, EXISTS((a)-[{val: 'foo'}]->(b)) AS con
          |RETURN a.id, b.id, con""".stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 2L, "con" -> true),
        CypherMap("a.id" -> 3L, "b.id" -> 4L, "con" -> false)
      ))
    }

    it("can evaluate simple exists pattern with node label predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE (v:SRC {id: 1})-[:REL]->(:A)
          |CREATE (w:SRC {id: 2})-[:REL]->(:B)
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (a:SRC)
          |WITH a, EXISTS((a)-->(:A)) AS con
          |RETURN a.id, con""".stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "con" -> true),
        CypherMap("a.id" -> 2L, "con" -> false)
      ))
    }

    it("can evaluate simple exists pattern with relationship type predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE (v {id: 1})-[:A]->({id: 2})<-[:REL]-(v)
          |CREATE (w {id: 3})-[:B]->({id: 4})<-[:REL]-(w)
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (a)-[:REL]->(b)
          |WITH a, b, EXISTS((a)-[:A]->(b)) AS con
          |RETURN a.id, b.id, con""".stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 2L, "con" -> true),
        CypherMap("a.id" -> 3L, "b.id" -> 4L, "con" -> false)
      ))
    }

    it("can evaluate inverse exist pattern") {
      // Given
      val given = initGraph("CREATE (v {id: 1})-[:REL]->({id: 2})")

      // When
      val result = given.cypher(
        """
          |MATCH (a), (b)
          |WITH a, b, NOT EXISTS((a)-->(b)) AS con
          |RETURN a.id, b.id, con""".stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "b.id" -> 1L, "con" -> true),
        CypherMap("a.id" -> 1L, "b.id" -> 2L, "con" -> false),
        CypherMap("a.id" -> 2L, "b.id" -> 1L, "con" -> true),
        CypherMap("a.id" -> 2L, "b.id" -> 2L, "con" -> true)
      ))
    }

    it("can evaluate exist pattern with derived node predicate") {
      // Given
      val given = initGraph(
        """
          |CREATE ({id: 1, val: 0})-[:REL]->({id: 2, val: 2})<-[:REL]-({id: 3, val: 10})
        """.stripMargin)

      // When
      val result = given.cypher(
        """
          |MATCH (a)
          |WITH a, EXISTS((a)-->({val: a.val + 2})) AS other RETURN a.id, other""".stripMargin)

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("a.id" -> 1L, "other" -> true),
        CypherMap("a.id" -> 2L, "other" -> false),
        CypherMap("a.id" -> 3L, "other" -> false)
      ))
    }
  }

  describe("ListLiteral") {
    it("can convert string ListLiterals from parameters") {
      val graph = initGraph("CREATE ()")

      val result = graph.cypher(
        """
          |WITH [$a, $b] as strings
          |RETURN strings""".stripMargin, Map("a" -> CypherValue("bar"), "b" -> CypherValue("foo")))

      result.records.toMaps should equal(Bag(
        CypherMap("strings" -> Seq("bar", "foo"))
      ))
    }

    it("can convert string ListLiterals") {
      val graph = initGraph("CREATE ()")

      val result = graph.cypher(
        """
          |WITH ["bar", "foo"] as strings
          |RETURN strings""".stripMargin)

      result.records.toMaps should equal(Bag(
        CypherMap("strings" -> Seq("bar", "foo"))
      ))
    }

    it("can convert ListLiterals with nested non literal expressions") {
      val graph = initGraph("CREATE ({val: 1}), ({val: 2})")

      val result = graph.cypher(
        """
          |MATCH (n)
          |WITH [n.val*10, n.val*100] as vals
          |RETURN vals""".stripMargin)

      result.records.toMaps should equal(Bag(
        CypherMap("vals" -> Seq(10, 100)),
        CypherMap("vals" -> Seq(20, 200))
      ))
    }

    it("can build lists that include nulls") {
      val result = morpheus.cypher(
        """
          |RETURN [
          | 1,
          | null
          |] AS p
        """.stripMargin)

      result.records.toMaps should equal(Bag(
        CypherMap("p" -> List(1, null))
      ))
    }
  }

  describe("ANDs") {
    it("can project ands") {
      val graph = initGraph(
        """
          |CREATE ({v1: true, v2: true, v3: true}), ({v1: false, v2: true, v3: true})
        """.stripMargin)

      val query =
        """
          | MATCH (n)
          | WHERE (n.v1 AND n.v2 AND n.v3) = true
          | RETURN n.v1
        """.stripMargin('|')

      graph.cypher(query).records.toMaps should equal(Bag(
        CypherMap("n.v1" -> true)
      ))
    }
  }

  describe("ContainerIndex") {
    it("Can extract the nth element from a list with literal index") {
      val graph = initGraph(
        """
          |CREATE ({v1: [1, 2, 3]})
        """.stripMargin)

      val query =
        """
          | MATCH (n)
          | RETURN n.v1[1] as val
        """.stripMargin('|')

      graph.cypher(query).records.toMaps should equal(Bag(
        CypherMap("val" -> 2)
      ))
    }

    it("Can extract the nth element from a list with expression index") {
      val graph = initGraph(
        """
          |CREATE ({v1: [1, 2, 3]})
        """.stripMargin)

      val query =
        """
          | MATCH (n)
          | UNWIND [0,1,2] as i
          | RETURN n.v1[i] as val
        """.stripMargin('|')

      graph.cypher(query).records.toMaps should equal(Bag(
        CypherMap("val" -> 1),
        CypherMap("val" -> 2),
        CypherMap("val" -> 3)
      ))
    }

    it("returns null when the index is out of bounds") {
      val graph = initGraph(
        """
          |CREATE ({v1: [1, 2, 3]})
        """.stripMargin)

      val query =
        """
          | MATCH (n)
          | UNWIND [3,4,5] as i
          | RETURN n.v1[i] as val
        """.stripMargin('|')

      graph.cypher(query).records.toMaps should equal(Bag(
        CypherMap("val" -> null),
        CypherMap("val" -> null),
        CypherMap("val" -> null)
      ))
    }
  }

  describe("string concatenation") {
    it("can concat two strings from literals") {
      morpheus.cypher(
        """
          |RETURN "Hello" + "World" as hello
        """.stripMargin).records.toMaps should equal(Bag(
        CypherMap("hello" -> "HelloWorld")
      ))
    }

    it("can concat two properties") {
      val g = initGraph(
        """
          |CREATE (:A {a: "Hello"})
          |CREATE (:B {b: "World"})
        """.stripMargin)

      g.cypher(
        """
          |MATCH (a:A), (b:B)
          |RETURN a.a + b.b AS hello
        """.stripMargin).records.toMaps should equal(Bag(
        CypherMap("hello" -> "HelloWorld")
      ))
    }

    it("can concat a string and an integer") {
      val g = initGraph(
        """
          |CREATE (:A {a1: "Hello", a2: 42})
          |CREATE (:B {b1: 42, b2: "Hello"})
        """.stripMargin)

      g.cypher(
        """
          |MATCH (a:A), (b:B)
          |RETURN a.a1 + b.b1 AS hello, a.a2 + b.b2 as world
        """.stripMargin).records.toMaps should equal(Bag(
        CypherMap("hello" -> "Hello42", "world" -> "42Hello")
      ))
    }

    it("can concat a string and a float") {
      val g = initGraph(
        """
          |CREATE (:A {a1: "Hello", a2: 42.0})
          |CREATE (:B {b1: 42.0, b2: "Hello"})
        """.stripMargin)

      g.cypher(
        """
          |MATCH (a:A), (b:B)
          |RETURN a.a1 + b.b1 AS hello, a.a2 + b.b2 as world
        """.stripMargin).records.toMaps should equal(Bag(
        CypherMap("hello" -> "Hello42.0", "world" -> "42.0Hello")
      ))
    }
  }

  describe("list concatenation") {

    it("can concat empty lists") {
      morpheus.cypher("RETURN [] + [] AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList())
      ))
    }

    it("can concat empty list with nonempty list") {
      morpheus.cypher("RETURN [] + ['foo'] AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList("foo"))
      ))
    }

    it("can concat list of null with nonnull scalar value") {
      morpheus.cypher("RETURN [null] + 'foo' AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(null, "foo"))
      ))
    }

    it("can concat empty list with scalar value") {
      morpheus.cypher("RETURN [] + '' AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(""))
      ))
    }

    it("can concat empty list with null scalar value") {
      morpheus.cypher("RETURN [] + null AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    it("can concat two literal lists of Cypher integers") {
      morpheus.cypher("RETURN [1] + [2] AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(1, 2))
      ))
    }

    it("can concat two literal lists of strings") {
      morpheus.cypher("RETURN ['foo'] + ['bar'] AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList("foo", "bar"))
      ))
    }

    it("can concat two literal lists of boolean type") {
      morpheus.cypher("RETURN [true] + [false] AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(true, false))
      ))
    }

    it("can concat two literal lists of float type") {
      morpheus.cypher("RETURN [0.5] + [1.5] AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(0.5, 1.5))
      ))
    }

    it("can concat two literal lists of date type") {
      val date = "2016-02-17"
      morpheus.cypher("RETURN [date($date)] + [date($date)] AS res", CypherMap("date" -> date))
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(java.sql.Date.valueOf(date), java.sql.Date.valueOf(date)))
      ))
    }

    it("can concat two literal lists of localdatetime type") {
      val date = "2016-02-17T06:11:00"
      morpheus.cypher("RETURN [localdatetime($date)] + [localdatetime($date)] AS res", CypherMap("date" -> date))
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(java.time.LocalDateTime.parse(date), java.time.LocalDateTime.parse(date)))
      ))
    }

    it("can concat two literal lists of duration type") {
      val duration = "P1WT2H"
      morpheus.cypher("RETURN [duration($duration)] + [duration($duration)] AS res", CypherMap("duration" -> duration))
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(Duration.parse(duration), Duration.parse(duration)))
      ))
    }

    it("can concat two literal lists of null type") {
      morpheus.cypher("RETURN [null] + [null] AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(null, null))
      ))
    }

    it("can concat two lists of nulls from expressions type") {
      morpheus.cypher("RETURN [acos(null)] + [acos(null)] AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(null, null))
      ))
    }

    it("can add integer literal to list of integer literals") {
      morpheus.cypher("RETURN [1] + 1 AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(1, 1))
      ))
    }

    it("can add string literal to list of string literals") {
      morpheus.cypher("RETURN ['hello'] + 'world' AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList("hello", "world"))
      ))
    }

    it("can add boolean literal to list of boolean literals") {
      morpheus.cypher("RETURN [true] + false AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(true, false))
      ))
    }

    it("can add float literal to list of float literals") {
      morpheus.cypher("RETURN [0.5] + 0.5 AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(0.5, 0.5))
      ))
    }

    it("can add date to list of dates") {
      val date = "2016-02-17"
      morpheus.cypher("RETURN [date($date)] + date($date) AS res", CypherMap("date" -> date))
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(java.sql.Date.valueOf(date), java.sql.Date.valueOf(date)))
      ))
    }

    it("can add localdatetime to list of localdatetime") {
      val date = "2016-02-17T06:11:00"
      morpheus.cypher("RETURN [localdatetime($date)] + localdatetime($date) AS res", CypherMap("date" -> date))
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(java.time.LocalDateTime.parse(date), java.time.LocalDateTime.parse(date)))
      ))
    }

    it("can add null literal to list of null literals") {
      morpheus.cypher("RETURN [null] + null AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> null)
      ))
    }

    it("can add empty list to string") {
      morpheus.cypher("RETURN 'hello' + [] AS res")
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList("hello"))
      ))
    }

  }

  describe("parameters") {

    it("can do list parameters") {
      morpheus.cypher("RETURN $listParam AS res", CypherMap("listParam" -> CypherList(1, 2)))
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList(1, 2))
      ))
    }

    it("throws exception on mixed types in list parameter") {
      val e = the[SparkSQLMappingException] thrownBy
        morpheus.cypher("RETURN $listParam AS res", CypherMap("listParam" -> CypherList(1, "string")))
          .records.toMaps
      e.getMessage should (include("LIST(UNION(INTEGER, STRING))") and include("unsupported"))
    }

    it("can support empty list parameter") {
      morpheus.cypher("RETURN $listParam AS res", CypherMap("listParam" -> CypherList()))
        .records.toMaps should equal(Bag(
        CypherMap("res" -> CypherList())
      ))
    }

  }

  describe("STARTS WITH") {
    it("returns true for matching strings") {
      morpheus.cypher(
        """
          |RETURN "foobar" STARTS WITH "foo" as x
        """.stripMargin
      ).records.toMaps should equal(Bag(
        CypherMap("x" -> true)
      ))
    }

    it("returns false for not matching strings") {
      morpheus.cypher(
        """
          |RETURN "foobar" STARTS WITH "bar" as x
        """.stripMargin
      ).records.toMaps should equal(Bag(
        CypherMap("x" -> false)
      ))
    }

    it("can handle nulls") {
      val g = initGraph(
        """
          |CREATE ({s: "foobar", r: null})
          |CREATE ({s: null, r: "foo"})
        """.stripMargin)

      g.cypher(
        """
          |MATCH (n)
          |RETURN n.s STARTS WITh n.r as x
        """.stripMargin
      ).records.toMaps should equal(Bag(
        CypherMap("x" -> null),
        CypherMap("x" -> null)
      ))
    }
  }

  describe("ENDS WITH") {
    it("returns true for matching strings") {
      morpheus.cypher(
        """
          |RETURN "foobar" ENDS WITH "bar" as x
        """.stripMargin
      ).records.toMaps should equal(Bag(
        CypherMap("x" -> true)
      ))
    }

    it("returns false for not matching strings") {
      morpheus.cypher(
        """
          |RETURN "foobar" ENDS WITH "foo" as x
        """.stripMargin
      ).records.toMaps should equal(Bag(
        CypherMap("x" -> false)
      ))
    }

    it("can handle nulls") {
      val g = initGraph(
        """
          |CREATE ({s: "foobar", r: null})
          |CREATE ({s: null, r: "bar"})
        """.stripMargin)

      g.cypher(
        """
          |MATCH (n)
          |RETURN n.s STARTS WITh n.r as x
        """.stripMargin
      ).records.toMaps should equal(Bag(
        CypherMap("x" -> null),
        CypherMap("x" -> null)
      ))
    }
  }

  describe("CONTAINS") {
    it("returns true for matching strings") {
      morpheus.cypher(
        """
          |RETURN "foobarbaz" CONTAINS "baz" as x
        """.stripMargin
      ).records.toMaps should equal(Bag(
        CypherMap("x" -> true)
      ))
    }

    it("returns false for not matching strings") {
      morpheus.cypher(
        """
          |RETURN "foobarbaz" CONTAINS "abc" as x
        """.stripMargin
      ).records.toMaps should equal(Bag(
        CypherMap("x" -> false)
      ))
    }

    it("can handle nulls") {
      val g = initGraph(
        """
          |CREATE ({s: "foobar", r: null})
          |CREATE ({s: null, r: "bar"})
        """.stripMargin)

      g.cypher(
        """
          |MATCH (n)
          |RETURN n.s STARTS WITh n.r as x
        """.stripMargin
      ).records.toMaps should equal(Bag(
        CypherMap("x" -> null),
        CypherMap("x" -> null)
      ))
    }
  }

  describe("properties") {
    it("can extract properties from nodes") {
      val g = initGraph(
        """
          |CREATE (:A {val1: "foo", val2: 42})
          |CREATE (:A {val1: "bar", val2: 21})
          |CREATE (:A)
        """.stripMargin)

      val result = g.cypher(
        """
          |MATCH (a:A)
          |RETURN properties(a) AS props
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("props" -> CypherMap("val1" -> "foo", "val2" -> 42)),
        CypherMap("props" -> CypherMap("val1" -> "bar", "val2" -> 21)),
        CypherMap("props" -> CypherMap("val1" -> null, "val2" -> null))
      ))
    }

    it("can extract properties from relationships") {
      val g = initGraph(
        """
          |CREATE (a), (b)
          |CREATE (a)-[:REL {val1: "foo", val2: 42}]->(b)
          |CREATE (a)-[:REL {val1: "bar", val2: 21}]->(b)
          |CREATE (a)-[:REL]->(b)
        """.stripMargin)

      val result = g.cypher(
        """
          |MATCH ()-[rel:REL]->()
          |RETURN properties(rel) as props
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("props" -> CypherMap("val1" -> "foo", "val2" -> 42)),
        CypherMap("props" -> CypherMap("val1" -> "bar", "val2" -> 21)),
        CypherMap("props" -> CypherMap("val1" -> null, "val2" -> null))
      ))
    }

    it("can extract properties from maps") {
      val g = initGraph(
        """
          |CREATE (a), (b)
          |CREATE (a)-[:REL {val1: "foo", val2: 42}]->(b)
          |CREATE (a)-[:REL {val1: "bar", val2: 21}]->(b)
        """.stripMargin)

      val result = g.cypher(
        """UNWIND [
          | {val1: "foo", val2: 42},
          | {val1: "bar", val2: 21}
          |] as map
          |RETURN properties(map) as props
        """.stripMargin).records

      result.toMaps should equal(Bag(
        CypherMap("props" -> CypherMap("val1" -> "foo", "val2" -> 42)),
        CypherMap("props" -> CypherMap("val1" -> "bar", "val2" -> 21))
      ))
    }

  }

  describe("map support") {
    describe("map construction") {
      it("can construct static maps") {
        val result = morpheus.cypher(
          """
            |RETURN {
            | foo: "bar",
            | baz: 42
            |} AS myMap
          """.stripMargin)

        result.records.toMaps should equal(Bag(
          CypherMap("myMap" -> Map("foo" -> "bar", "baz" -> 42))
        ))
      }

      it("can construct Maps with expression values") {
        val result = morpheus.cypher(
          """
            |UNWIND [21, 42] as value
            |RETURN {foo: value} as myMap
          """.stripMargin)

        result.records.toMaps should equal(Bag(
          CypherMap("myMap" -> Map("foo" -> 21)),
          CypherMap("myMap" -> Map("foo" -> 42))
        ))
      }

      it("can construct nodes with map properties") {
        val g = initGraph(
          """
            |CREATE (:A {val: "foo"})
          """.stripMargin)

        val result = g.cypher(
          """
            |MATCH (a:A)
            |CONSTRUCT
            | CREATE (b {map: {val: a.val}})
            |MATCH (n)
            |RETURN n.map as map
          """.stripMargin).records

        result.toMaps should equal(Bag(
          CypherMap("map" -> CypherMap("val" -> "foo"))
        ))
      }

      it("can return empty maps") {
        val result = morpheus.cypher(
          """
            |RETURN {} as myMap
          """.stripMargin)

        result.records.toMaps should equal(Bag(
          CypherMap("myMap" -> CypherMap())
        ))
      }
    }


    describe("index access") {
      it("returns the element with literal key") {
        val result = morpheus.cypher(
          """
            |WITH {
            | foo: "bar",
            | baz: 42
            |} as myMap
            |RETURN myMap["foo"] as foo, myMap["baz"] as baz
          """.stripMargin)

        result.records.toMaps should equal(Bag(
          CypherMap("foo" -> "bar", "baz" -> 42)
        ))
      }

      it("returns null if the literal key does not exist") {
        val result = morpheus.cypher(
          """
            |WITH {
            | foo: "bar",
            | baz: 42
            |} as myMap
            |RETURN myMap["barbaz"] as barbaz
          """.stripMargin)

        result.records.toMaps should equal(Bag(
          CypherMap("barbaz" -> null)
        ))
      }

      it("returns the element with parameter key") {
        val result = morpheus.cypher(
          """
            |WITH {
            | foo: "bar",
            | baz: 42
            |} as myMap
            |RETURN myMap[$fooKey] as foo, myMap[$bazKey] as baz
          """.stripMargin, CypherMap("fooKey" -> "foo", "bazKey" -> "baz"))

        result.records.toMaps should equal(Bag(
          CypherMap("foo" -> "bar", "baz" -> 42)
        ))
      }

      // TODO: This throws a spark analysis error as it cannot find the column
      ignore("returns null if the parameter key does not exist") {
        val result = morpheus.cypher(
          """
            |WITH {
            | foo: "bar",
            | baz: 42
            |} as myMap
            |RETURN myMap[$barbazKey] as barbaz
          """.stripMargin, CypherMap("barbazKey" -> "barbaz"))

        result.records.toMaps should equal(Bag(
          CypherMap("barbaz" -> null)
        ))
      }

      // TODO: needs planning outside of SparkSQLExpressionMapper
      ignore("supports expression keys if all values have compatible types") {
        val result = morpheus.cypher(
          """
            |WITH {
            | foo: 1,
            | bar: 2
            |} as myMap
            |UNWIND ["foo", "bar"] as key
            |RETURN myMap[key] as value
          """.stripMargin)

        result.records.toMaps should equal(Bag(
          CypherMap("value" -> 1),
          CypherMap("value" -> 2)
        ))
      }
    }
  }

  describe("list comprehension") {
    it("supports list comprehension with static mapping") {
      val result = morpheus.cypher(
        """
          |WITH [1, 2, 3] AS things
          |RETURN [n IN things | 1] AS value
        """.stripMargin)

      result.records.toMaps shouldEqual Bag(
        CypherMap("value" -> List(1, 1, 1))
      )
    }

    it("supports list comprehension with simple mapping") {
      val result = morpheus.cypher(
        """
          |WITH [1, 2, 3] AS things
          |RETURN [n IN things | n*3] AS value
        """.stripMargin)

      result.records.toMaps shouldEqual Bag(
        CypherMap("value" -> List(3, 6, 9))
      )
    }

    it("supports list comprehension with more complex mapping") {
      val result = morpheus.cypher(
        """
          |WITH ['1', '2', '3'] AS things
          |RETURN [n IN things | toInteger(n)*3 + toInteger(n)] AS value
        """.stripMargin)

      result.records.toMaps shouldEqual Bag(
        CypherMap("value" -> List(4, 8, 12))
      )
    }

    it("supports list comprehension with inner predicate") {
      val result = morpheus.cypher(
        """
          |WITH [1, 2, 3] AS things
          |RETURN [n IN things WHERE n > 2] AS value
        """.stripMargin)

      result.records.toMaps shouldEqual Bag(
        CypherMap("value" -> List(3))
      )
    }

    it("supports list comprehension with inner predicate and more complex mapping") {
      val result = morpheus.cypher(
        """
          |WITH ['1', '2', '3'] AS things
          |RETURN [n IN things WHERE toInteger(n) > 2 | toInteger(n)*3 + toInteger(n)] AS value
        """.stripMargin)

      result.records.toMaps shouldEqual Bag(
        CypherMap("value" -> List(12))
      )
    }

    it("supports nested list comprehensions") {
      val result = morpheus.cypher(
        """
          |WITH [[1,2,3], [2,2,3], [3,4]] AS things
          |RETURN [n IN things | [n IN n WHERE n < 2]] AS value
        """.stripMargin)

      result.records.toMaps shouldEqual Bag(
        CypherMap("value" -> List(List(1), List(), List()))
      )
    }
  }
}
