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
package org.opencypher.okapi.ir.impl.parse.rewriter

import org.opencypher.okapi.ir.test.support.RewriterTestSupport
import org.opencypher.okapi.test.BaseTestSuite

class normalizeCaseExpressionTest extends BaseTestSuite with RewriterTestSupport {
  override val rewriter = normalizeCaseExpression

  it("should rewrite simple CASE statement") {
    assertRewrite(
      """
        |MATCH (n)
        |RETURN
        |  CASE n.val
        |    WHEN "foo" THEN 1
        |    WHEN "bar" THEN 2
        |  END AS val
      """.stripMargin,
      """
        |MATCH (n)
        |RETURN
        |  CASE
        |    WHEN n.val = "foo" THEN 1
        |    WHEN n.val = "bar" THEN 2
        |  END AS val
      """.stripMargin)
  }

  it("should rewrite simple CASE statement with default") {
    assertRewrite(
      """
        |MATCH (n)
        |RETURN
        |  CASE n.val
        |    WHEN "foo" THEN 1
        |    WHEN "bar" THEN 2
        |    ELSE 3
        |  END AS val
      """.stripMargin,
      """
        |MATCH (n)
        |RETURN
        |  CASE
        |    WHEN n.val = "foo" THEN 1
        |    WHEN n.val = "bar" THEN 2
        |    ELSE 3
        |  END AS val
      """.stripMargin)
  }

  it("should not rewrite generic CASE statement") {
    assertRewrite(
      """
        |MATCH (n)
        |RETURN
        |  CASE
        |    WHEN n.val = "foo" THEN 1
        |    WHEN n.val = "bar" THEN 2
        |    ELSE 3
        |  END AS val
      """.stripMargin,
      """
        |MATCH (n)
        |RETURN
        |  CASE
        |    WHEN n.val = "foo" THEN 1
        |    WHEN n.val = "bar" THEN 2
        |    ELSE 3
        |  END AS val
      """.stripMargin)
  }
}
