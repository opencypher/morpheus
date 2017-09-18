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

import scala.collection.immutable.Bag

class MultigraphProjectionAcceptanceTest extends CAPSTestSuite {

  private val testGraph1 = TestGraph("(:Person {name: 'Mats'})")
  private val testGraph2 = TestGraph("(:Person {name: 'Phil'})")
  private val testGraph3 = TestGraph("(:Car {type: 'Toyota'})")

  test("returning a graph") {
    testGraph1.mountAt("/test/graph1")
    testGraph2.mountAt("/test/graph2")

    val query =
      """FROM GRAPH AT '/test/graph2' AS myGraph
        |MATCH (n:Person)
        |RETURN n.name AS name GRAPHS myGraph""".stripMargin

    val result = testGraph1.graph.cypher(query)

    result.records.toMaps should equal(Bag(
      CypherMap("name" -> "Phil")
    ))

    result.graphs shouldMatch testGraph2.graph
  }

  test("Can select a source graph to match data from") {
    testGraph1.mountAt("/test/graph1")
    testGraph2.mountAt("/test/graph2")

    val query =
      """WITH * GRAPHS *, GRAPH myGraph AT '/test/graph2' >>
        |MATCH (n:Person)
        |RETURN n.name AS name""".stripMargin

    val result = testGraph1.graph.cypher(query)

    result.records.toMaps should equal(Bag(
      CypherMap("name" -> "Phil")
    ))

    result.graphs shouldBe empty
  }

  test("Can select a source graph to match data from (syntactic sugar variant)") {
    testGraph1.mountAt("/test/graph1")
    testGraph2.mountAt("/test/graph2")

    val query =
      """FROM GRAPH myGraph AT '/test/graph2'
        |MATCH (n:Person)
        |RETURN n.name AS name""".stripMargin

    val result = testGraph1.graph.cypher(query)

    result.records.toMaps should equal(Bag(
      CypherMap("name" -> "Phil")
    ))

    result.graphs shouldBe empty
  }

  ignore("matching from different graphs") {
    testGraph1.mountAt("/test/graph1")
    testGraph2.mountAt("/test/graph2")
    testGraph3.mountAt("/test/graph3")

    val query =
      """FROM GRAPH myGraph AT '/test/graph2'
        |MATCH (n:Person)
        |FROM GRAPH another AT '/test/graph3'
        |MATCH (c:Car)
        |RETURN n.name AS name, c.type AS car""".stripMargin

    val result = testGraph1.graph.cypher(query)

    result.records.toMaps should equal(Bag(
      CypherMap("name" -> "Phil", "car" -> "Toyota")
    ))
    result.graphs shouldBe empty
  }
}
