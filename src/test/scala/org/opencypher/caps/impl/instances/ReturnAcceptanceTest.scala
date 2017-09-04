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
package org.opencypher.caps.impl.instances

import org.opencypher.caps.CAPSTestSuite
import org.opencypher.caps.api.value.CypherMap

import scala.collection.Bag

class ReturnAcceptanceTest extends CAPSTestSuite {

  test("return node") {
    val given = TestGraph("({foo:'bar'}),()")

    val result = given.cypher("MATCH (n) RETURN n")

    result.records.toMaps should equal(Bag(
      CypherMap("n" -> 0),
      CypherMap("n" -> 1))
    )
  }

  test("return node with details") {
    val given = TestGraph("({foo:'bar'}),()")

    val result = given.cypher("MATCH (n) RETURN n")

    result.recordsWithDetails.toMaps should equal(Bag(
      CypherMap("n" -> 0, s"n:$DEFAULT_LABEL" -> true, "n.foo" -> "bar"),
      CypherMap("n" -> 1, s"n:$DEFAULT_LABEL" -> true, "n.foo" -> null))
    )
  }

  test("return rel") {
    val given = TestGraph("()-[{foo:'bar'}]->()-[]->()")

    val result = given.cypher("MATCH ()-[r]->() RETURN r")

    result.records.toMaps should equal(Bag(
      CypherMap("r" -> 0),
      CypherMap("r" -> 1)
    ))
  }

  test("return rel with details") {
    val given = TestGraph("()-[{foo:'bar'}]->()-[]->()").graph

    val result = given.cypher("MATCH ()-[r]->() RETURN r")

    val relId = given.tokens.registry.relTypeRefByName(DEFAULT_LABEL).id

    result.recordsWithDetails.toMaps should equal(Bag(
      CypherMap("r" -> 0, "source(r)" -> 0, "target(r)" -> 1, "type(r)" -> relId, "r.foo" -> "bar"),
      CypherMap("r" -> 1, "source(r)" -> 1, "target(r)" -> 2, "type(r)" -> relId, "r.foo" -> null)
    ))
  }
}
