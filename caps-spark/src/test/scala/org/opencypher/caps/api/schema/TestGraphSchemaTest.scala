/*
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
package org.opencypher.caps.api.schema

import org.opencypher.caps.api.types.{CTInteger, CTString}
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.GraphCreationFixture

class TestGraphSchemaTest extends CAPSTestSuite with GraphCreationFixture {

  test("constructs schema correctly for unlabeled nodes") {
    val graph = initGraph("CREATE ({id: 1}), ({id: 2}), ({other: 'foo'}), ()")

    graph.schema should equal(Schema.empty
      .withNodePropertyKeys(Schema.NoLabel, Map("id" -> CTInteger.nullable, "other" -> CTString.nullable))
    )
  }

  test("constructs schema correctly for labeled nodes") {
    val graph = initGraph("CREATE (:A {id: 1}), (:A {id: 2}), (:B {other: 'foo'})")

    graph.schema should equal(Schema.empty
      .withNodePropertyKeys("A")("id" -> CTInteger)
      .withNodePropertyKeys("B")("other" -> CTString)
    )
  }

  test("constructs schema correctly for multi-labeled nodes") {
    val graph = initGraph("CREATE (:A {id: 1}), (:A:B {id: 2}), (:B {other: 'foo'})")

    graph.schema should equal(Schema.empty
      .withNodePropertyKeys("A")("id" -> CTInteger)
      .withNodePropertyKeys("B")("other" -> CTString)
      .withNodePropertyKeys("A", "B")("id" -> CTInteger)
    )
  }

  test("constructs schema correctly for relationships") {
    val graph = initGraph(
      """
        |CREATE ()-[:FOO {p: 1}]->()
        |CREATE ()-[:BAR {p: 2, q: 'baz'}]->()
        |CREATE ()-[:BAR {p: 3}]->()
      """.stripMargin
    )

    graph.schema should equal(Schema.empty
      .withNodePropertyKeys(Schema.NoLabel, PropertyKeys.empty)
      .withRelationshipPropertyKeys("FOO")("p" -> CTInteger)
      .withRelationshipPropertyKeys("BAR")("p" -> CTInteger, "q" -> CTString.nullable)
    )
  }
}
