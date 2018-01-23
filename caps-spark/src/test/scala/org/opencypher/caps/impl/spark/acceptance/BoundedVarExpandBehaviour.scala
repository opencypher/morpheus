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

import org.opencypher.caps.api.value.{CypherList, CypherMap}
import org.opencypher.caps.impl.spark.CAPSGraph

import scala.collection.Bag

trait BoundedVarExpandBehaviour {
    this: AcceptanceTest =>

  def boundedVarExpandBehaviour(initGraph: String => CAPSGraph): Unit = {
    test("bounded to single relationship") {

      // Given
      val given = initGraph("CREATE (:Node {val: 'source'})-[:REL]->(:Node {val: 'mid1'})-[:REL]->(:Node {val: 'end'})")

      // When
      val result = given.cypher("MATCH (n:Node)-[r*0..1]->(m:Node) RETURN m.val")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("m.val" -> "source"),
        CypherMap("m.val" -> "mid1"),
        CypherMap("m.val" -> "mid1"),
        CypherMap("m.val" -> "end"),
        CypherMap("m.val" -> "end")
      ))

      // And
      result.graphs shouldBe empty
    }

    test("bounded with lower bound") {

      // Given
      val given = initGraph("CREATE (:Node {val: 'source'})-[:REL]->(:Node {val: 'mid1'})-[:REL]->(:Node {val: 'end'})")

      // When
      val result = given.cypher("MATCH (t:Node)-[r*2..3]->(y:Node) RETURN y.val")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("y.val" -> "end")
      ))

      // And
      result.graphs shouldBe empty
    }

    test("var expand with default lower and loop") {
      // Given
      val given = initGraph("CREATE (a:Node {v: 'a'})-[:REL]->(:Node {v: 'b'})-[:REL]->(:Node {v: 'c'})-[:REL]->(a)")

      // When
      val result = given.cypher("MATCH (a:Node)-[r*..6]->(b:Node) RETURN b.v")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("b.v" -> "a"),
        CypherMap("b.v" -> "a"),
        CypherMap("b.v" -> "a"),
        CypherMap("b.v" -> "b"),
        CypherMap("b.v" -> "b"),
        CypherMap("b.v" -> "b"),
        CypherMap("b.v" -> "c"),
        CypherMap("b.v" -> "c"),
        CypherMap("b.v" -> "c")
      ))

      // And
      result.graphs shouldBe empty
    }

    test("var expand return list of rel ids") {
      // Given
      val given = initGraph("CREATE (a:Node {v: 'a'})-[:REL]->(:Node {v: 'b'})-[:REL]->(:Node {v: 'c'})-[:REL]->(a)")

      // When
      val result = given.cypher("MATCH (a:Node)-[r*..6]->(b:Node) RETURN r")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("r" -> CypherList(Seq(2))),
        CypherMap("r" -> CypherList(Seq(2, 4))),
        CypherMap("r" -> CypherList(Seq(2, 4, 5))),
        CypherMap("r" -> CypherList(Seq(4))),
        CypherMap("r" -> CypherList(Seq(4, 5))),
        CypherMap("r" -> CypherList(Seq(4, 5, 2))),
        CypherMap("r" -> CypherList(Seq(5))),
        CypherMap("r" -> CypherList(Seq(5, 2))),
        CypherMap("r" -> CypherList(Seq(5, 2, 4)))
      ))

      // And
      result.graphs shouldBe empty
    }

    test("var expand with rel type") {
      // Given
      val given = initGraph("CREATE (a:Node {v: 'a'})-[:LOVES]->(:Node {v: 'b'})-[:KNOWS]->(:Node {v: 'c'})-[:HATES]->(a)")

      // When
      val result = given.cypher("MATCH (a:Node)-[r:LOVES|KNOWS*..6]->(b:Node) RETURN b.v")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("b.v" -> "b"),
        CypherMap("b.v" -> "c"),
        CypherMap("b.v" -> "c")
      ))

      // And
      result.graphs shouldBe empty
    }

    // Property predicates on var-length patterns get rewritten in AST to WHERE all(_foo IN r | _foo.prop = value)
    // We could do that better by pre-filtering candidate relationships prior to the iterate step
    ignore("var expand with property filter") {
      // Given
      val given = initGraph("CREATE (a:Node {v: 'a'})-[{v: 1L}]->(:Node {v: 'b'})-[{v: 2L}]->(:Node {v: 'c'})-[{v: 2L}]->(a)")

      // When
      val result = given.cypher("MATCH (a:Node)-[r*..6 {v: 2}]->(b:Node) RETURN b.v")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("b.v" -> "c"),
        CypherMap("b.v" -> "a"),
        CypherMap("b.v" -> "a")
      ))

      // And
      result.graphs shouldBe empty
    }

    test("var expand with additional hop") {
      // Given
      val given = initGraph("CREATE (a:Node {v: 'a'})-[:KNOWS]->(:Node {v: 'b'})-[:KNOWS]->(:Node {v: 'c'})-[:HATES]->(d:Node {v: 'd'})")

      // When
      val result = given.cypher("MATCH (a:Node)-[r:KNOWS*..6]->(b:Node)-[:HATES]->(c:Node) RETURN c.v")

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("c.v" -> "d"),
        CypherMap("c.v" -> "d")
      ))

      // And
      result.graphs shouldBe empty
    }

    test("var expand with expand into") {
      // Given
      val given = initGraph(
        """
          |CREATE (a:Person {name: "Philip"})
          |CREATE (b:Person {name: "Stefan"})
          |CREATE (c:City {name: "Berlondon"})
          |CREATE (a)-[:KNOWS]->(b)
          |CREATE (a)-[:LIVES_IN]->(c)
          |CREATE (b)-[:LIVES_IN]->(c)
        """.stripMargin)

      val result = given.cypher(
        """
          |MATCH (a:Person)-[:LIVES_IN]->(c:City)<-[:LIVES_IN]-(b:Person),
          |(a)-[:KNOWS*1..2]->(b) RETURN a.name, b.name, c.name
        """.stripMargin
      )

      // Then
      result.records.toMaps should equal(Bag(
        CypherMap("a.name" -> "Philip", "b.name" -> "Stefan", "c.name" -> "Berlondon")
      ))
    }
  }
}
