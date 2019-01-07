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
package org.opencypher.okapi.ir.impl.parse.rewriter

import org.opencypher.okapi.ir.test.support.RewriterTestSupport
import org.opencypher.okapi.testing.BaseTestSuite

class pushLabelsIntoScansTest extends BaseTestSuite with RewriterTestSupport {
  override val rewriter: pushLabelsIntoScans.type = pushLabelsIntoScans

  test("push node label from WHERE clause into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Foo
        |RETURN n
      """.stripMargin,
      """MATCH (n:Foo)
        |RETURN n
      """.stripMargin)
  }

  test("push labels for multiple nodes into pattern") {
    assertRewrite(
      """MATCH (n:Foo)-[:Rel]->(b:Foo:Bar)
        |RETURN n
      """.stripMargin,
      """MATCH (n:Foo)-[:Rel]->(b:Foo:Bar)
        |RETURN n
      """.stripMargin)
  }

  test("keep the node label in the pattern") {
    assertRewrite(
      """MATCH (n:Foo)
        |RETURN n
      """.stripMargin,
      """MATCH (n:Foo)
        |RETURN n
      """.stripMargin)
  }

  test("push additional labels into pattern") {
    assertRewrite(
      """MATCH (n:Foo)
        |WHERE n:Bar
        |RETURN n
      """.stripMargin,
      """MATCH (n:Foo:Bar)
        |RETURN n
      """.stripMargin)
  }

  test("push complex where predicate labels into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Bar AND n:Baz
        |RETURN n
      """.stripMargin,
      """MATCH (n:Bar:Baz)
        |RETURN n
      """.stripMargin)
  }

  test("push label predicates from complex where predicate into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Bar AND n.age > 25
        |RETURN n
      """.stripMargin,
      """MATCH (n:Bar)
        |WHERE n.age > 25
        |RETURN n
      """.stripMargin)
  }

  test("do not push OR'ed labels into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Bar OR n:Baz
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WHERE n:Bar OR n:Baz
        |RETURN n
      """.stripMargin)
  }

  test("do not push ORS'ed labels into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Foo OR n:Bar OR n:Baz
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WHERE n:Foo OR n:Bar OR n:Baz
        |RETURN n
      """.stripMargin)
  }

  test("do not push NOT'ed labels into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE NOT n:Foo
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WHERE NOT n:Foo
        |RETURN n
      """.stripMargin
    )
  }

  test("do not push XOR'ed labels into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Foo XOR n:Bar
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WHERE n:Foo XOR n:Bar
        |RETURN n
      """.stripMargin
    )
  }

}
