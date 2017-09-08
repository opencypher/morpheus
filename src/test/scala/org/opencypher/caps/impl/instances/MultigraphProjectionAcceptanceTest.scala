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

import scala.collection.immutable.Bag

class MultigraphProjectionAcceptanceTest extends CAPSTestSuite {

  ignore("Can select a source graph to match data from") {
    testGraph1.mountAt("/test/graph1")
    testGraph2.mountAt("/test/graph2")
    val result = testGraph1.graph.cypher("FROM GRAPH myGraph AT '/test/graph2' MATCH (n:Person) RETURN n.name AS name")

    result.records.toMaps should equal(Bag(
      CypherMap("name" -> "Phil")
    ))
  }

  private def testGraph1 = TestGraph("(a:Person {name: 'Mats'})")
  private def testGraph2 = TestGraph("(a:Person {name: 'Phil'})")
}
