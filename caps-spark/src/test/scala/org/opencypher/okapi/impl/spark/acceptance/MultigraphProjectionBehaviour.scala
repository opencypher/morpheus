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
package org.opencypher.okapi.impl.spark.acceptance

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.spark.CAPSConverters._
import org.opencypher.okapi.impl.spark.CAPSGraph

import scala.collection.immutable.Bag

trait MultigraphProjectionBehaviour { this: AcceptanceTest =>

  def multigraphProjectionBehaviour(initGraph: String => CAPSGraph): Unit = {
    def testGraph1 = initGraph("CREATE (:Person {name: 'Mats'})")
    def testGraph2 = initGraph("CREATE (:Person {name: 'Phil'})")
    def testGraph3 = initGraph("CREATE (:Car {type: 'Toyota'})")

    test("returning a graph") {
      caps.store(GraphName("graph1"), testGraph1)
      caps.store(GraphName("graph2"), testGraph2)

      val query =
        """FROM GRAPH AT 'graph2' AS myGraph
          |MATCH (n:Person)
          |RETURN n.name AS name GRAPHS myGraph""".stripMargin

      val result = testGraph1.cypher(query)

      result.records.toMaps should equal(
        Bag(
          CypherMap("name" -> "Phil")
        ))

      result.asCaps.graphs shouldMatch testGraph2
    }

    test("Can select a source graph to match data from") {
      caps.store(GraphName("graph1"), testGraph1)
      caps.store(GraphName("graph2"), testGraph2)

      val query =
        """WITH * GRAPHS *, GRAPH myGraph AT 'graph2' >>
          |MATCH (n:Person)
          |RETURN n.name AS name""".stripMargin

      val result = testGraph1.cypher(query)

      result.records.toMaps should equal(
        Bag(
          CypherMap("name" -> "Phil")
        ))

      result.graphs shouldBe empty
    }

    test("Can select a source graph to match data from (syntactic sugar variant)") {
      caps.store(GraphName("graph1"), testGraph1)
      caps.store(GraphName("graph2"), testGraph2)

      val query =
        """FROM GRAPH myGraph AT 'graph2'
          |MATCH (n:Person)
          |RETURN n.name AS name""".stripMargin

      val result = testGraph1.cypher(query)

      result.records.toMaps should equal(
        Bag(
          CypherMap("name" -> "Phil")
        ))

      result.graphs shouldBe empty
    }

    test("matching from different graphs") {
      caps.store(GraphName("graph1"), testGraph1)
      caps.store(GraphName("graph2"), testGraph2)
      caps.store(GraphName("graph3"), testGraph3)

      val query =
        """FROM GRAPH myGraph AT 'graph2'
          |MATCH (n:Person)
          |WITH n.name AS name
          |FROM GRAPH another AT 'graph3'
          |MATCH (c:Car)
          |RETURN name, c.type AS car""".stripMargin

      val result = testGraph1.cypher(query)

      result.records.toMaps should equal(
        Bag(
          CypherMap("name" -> "Phil", "car" -> "Toyota")
        ))
      result.graphs shouldBe empty
    }
  }

}
