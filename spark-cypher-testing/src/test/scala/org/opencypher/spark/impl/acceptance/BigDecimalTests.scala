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

import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.testing.MorpheusTestSuite

class BigDecimalTests extends MorpheusTestSuite with ScanGraphInit {

  describe("general") {

    it("returns a big decimal") {
      morpheus.cypher("RETURN bigdecimal(1234, 4, 2) AS decimal").records.toMaps should equal(
        Bag(
          CypherMap("decimal" -> BigDecimal(1234, 2))
        )
      )
    }
  }

  describe("arithmetics") {

    it("adds two big decimals") {
      morpheus.cypher("RETURN bigdecimal(1234, 4, 2) + bigdecimal(12, 2, 1) AS decimal").records.toMaps should equal(
        Bag(
          CypherMap("decimal" -> BigDecimal(1354, 2))
        )
      )
    }

    it("adds a big decimal and an integer") {
      morpheus.cypher("RETURN bigdecimal(1234, 4, 2) + 10 AS decimal").records.toMaps should equal(
        Bag(
          CypherMap("decimal" -> BigDecimal(2234, 2))
        )
      )

      morpheus.cypher("RETURN 10 + bigdecimal(1234, 4, 2) AS decimal").records.toMaps should equal(
        Bag(
          CypherMap("decimal" -> BigDecimal(2234, 2))
        )
      )
    }

    it("adds a big decimal and a float") {
      morpheus.cypher("RETURN bigdecimal(1234, 4, 2) + 10.2 AS decimal").records.toMaps should equal(
        Bag(
          CypherMap("decimal" -> 22.54)
        )
      )

      morpheus.cypher("RETURN 10.2 + bigdecimal(1234, 4, 2) AS decimal").records.toMaps should equal(
        Bag(
          CypherMap("decimal" -> 22.54)
        )
      )
    }

    it("subtracts a big decimal and an integer") {
      morpheus.cypher("RETURN bigdecimal(1234, 4, 2) - 10 AS decimal").records.toMaps should equal(
        Bag(
          CypherMap("decimal" -> BigDecimal(234, 2))
        )
      )

      morpheus.cypher("RETURN 10 - bigdecimal(1234, 4, 2) AS decimal").records.toMaps should equal(
        Bag(
          CypherMap("decimal" -> BigDecimal(-234, 2))
        )
      )
    }

    it("subtracts a big decimal and a float") {
      morpheus.cypher("RETURN bigdecimal(44, 2, 1) - 0.2 AS decimal").records.toMaps should equal(
        Bag(
          CypherMap("decimal" -> 4.2)
        )
      )
    }
  }

}
