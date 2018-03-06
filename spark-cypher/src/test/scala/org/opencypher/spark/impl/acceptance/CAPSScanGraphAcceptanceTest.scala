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
package org.opencypher.spark.impl.acceptance

import org.opencypher.okapi.api.value.CypherValue.{CypherList, CypherMap}
import org.opencypher.spark.test.support.creation.caps.{CAPSScanGraphFactory, CAPSTestGraphFactory}

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSTestGraphFactory = CAPSScanGraphFactory

  it("bar") {
    val graph = initGraph("CREATE ()")

    val result = graph.cypher(
      """RETURN $elt IN $coll AS result""",
      CypherMap("elt" -> null, "coll" -> CypherList())
    )

    result.show
  }
}

/**
cenario Outline: Using null in IN
    And parameters are:
      | elt    | <elt>    |
      | coll   | <coll>   |
    When executing query:
      """
      RETURN $elt IN $coll AS result
      """
    Then the result should be:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | elt  | coll            | result |
      | null | null            | null   |
      | null | [1, 2, 3]       | null   |
      | null | [1, 2, 3, null] | null   |
      | null | []              | false  |
      | 1    | [1, 2, 3, null] | true   |
      | 1    | [null, 1]       | true   |
      | 5    | [1, 2, 3, null] | null   | **/