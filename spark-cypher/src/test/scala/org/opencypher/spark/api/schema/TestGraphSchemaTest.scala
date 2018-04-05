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
package org.opencypher.okapi.api.schema

import org.opencypher.okapi.api.types.{CTInteger, CTString}
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.GraphConstructionFixture

class TestGraphSchemaTest extends CAPSTestSuite with GraphConstructionFixture {

  test("constructs schema correctly for unlabeled nodes") {
    val graph = initGraph("CREATE ({id: 1}), ({id: 2}), ({other: 'foo'}), ()")

    graph.schema should equal(Schema.empty
      .withNodePropertyKeys(Set.empty[String], Map("id" -> CTInteger.nullable, "other" -> CTString.nullable))
      .asCaps
    )
  }

  test("constructs schema correctly for labeled nodes") {
    val graph = initGraph("CREATE (:A {id: 1}), (:A {id: 2}), (:B {other: 'foo'})")

    graph.schema should equal(Schema.empty
      .withNodePropertyKeys("A")("id" -> CTInteger)
      .withNodePropertyKeys("B")("other" -> CTString)
      .asCaps
    )
  }

  test("constructs schema correctly for multi-labeled nodes") {
    val graph = initGraph("CREATE (:A {id: 1}), (:A:B {id: 2}), (:B {other: 'foo'})")

    graph.schema should equal(Schema.empty
      .withNodePropertyKeys("A")("id" -> CTInteger)
      .withNodePropertyKeys("B")("other" -> CTString)
      .withNodePropertyKeys("A", "B")("id" -> CTInteger)
      .asCaps
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
      .withNodePropertyKeys(Set.empty[String], PropertyKeys.empty)
      .withRelationshipPropertyKeys("FOO")("p" -> CTInteger)
      .withRelationshipPropertyKeys("BAR")("p" -> CTInteger, "q" -> CTString.nullable)
      .asCaps
    )
  }
}
