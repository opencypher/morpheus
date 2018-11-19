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

import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.api.value.CAPSNode
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.DoNotDiscover

@DoNotDiscover
class UnionAllBehaviour extends CAPSTestSuite with DefaultGraphInit {

  describe("tabular union all") {
    it("unions simple queries") {
      val result = caps.cypher(
        """
          |RETURN 1 AS one
          |UNION ALL
          |RETURN 2 AS one
        """.stripMargin).records

      result.toMapsWithCollectedEntities should equal(Bag(
        CypherMap("one" -> 1),
        CypherMap("one" -> 2)
      ))
    }

    it("supports stacked union all") {
      val result = caps.cypher(
        """
          |RETURN 1 AS one
          |UNION ALL
          |RETURN 2 AS one
          |UNION ALL
          |RETURN 2 AS one
          |UNION ALL
          |RETURN 3 AS one
        """.stripMargin).records

      result.toMapsWithCollectedEntities should equal(Bag(
        CypherMap("one" -> 1),
        CypherMap("one" -> 2),
        CypherMap("one" -> 2),
        CypherMap("one" -> 3)
      ))
    }

    it("supports union all with UNWIND") {
      val result = caps.cypher(
        """
          |UNWIND [1, 2] AS i
          |RETURN i
          |UNION ALL
          |UNWIND [1, 2, 6] AS i
          |RETURN i
        """.stripMargin).records

      result.toMapsWithCollectedEntities should equal(Bag(
        CypherMap("i" -> 1),
        CypherMap("i" -> 2),
        CypherMap("i" -> 1),
        CypherMap("i" -> 2),
        CypherMap("i" -> 6)
      ))
    }

    it("supports union all with MATCH") {
      val g = initGraph(
        """
          |CREATE (a: A {val: "foo"})
          |CREATE (b: B {bar: "baz"})
        """.stripMargin)

      val result = g.cypher(
        """
          |MATCH (a:A)
          |RETURN a AS node
          |UNION ALL
          |MATCH (b:B)
          |RETURN b AS node
        """.stripMargin).records

      result.toMapsWithCollectedEntities should equal(Bag(
        CypherMap("node" -> CAPSNode(0, Set("A"), CypherMap("val" -> "foo"))),
        CypherMap("node" -> CAPSNode(1, Set("B"), CypherMap("bar" -> "baz")))
      ))
    }
  }
}
