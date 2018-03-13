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
package org.opencypher.okapi.ir.impl.parse.rewriter

import org.opencypher.okapi.ir.test.support.RewriterTestSupport
import org.opencypher.okapi.test.BaseTestSuite

class normalizeReturnClausesTest extends BaseTestSuite with RewriterTestSupport {
  override val rewriter = normalizeReturnClauses

  test("do not rewrite unaliased return items of variables") {
    assertRewrite(
      """MATCH (n)
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |RETURN n
      """.stripMargin)
  }

  test("do not rewrite aliased return items of variables") {
    assertRewrite(
      """MATCH (n)
        |RETURN n AS foo
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS foo
      """.stripMargin)
  }

  test("do no rewrite unaliased return item of properties") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop
      """.stripMargin)
  }

  test("do no rewrite aliased return item of properties") {
    assertRewrite(
      """MATCH (n)
        |RETURN n.prop AS foo
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS `foo`
        |RETURN `foo` AS `foo`
      """.stripMargin
    )
  }

  test("introduce WITH clause for unaliased non-primitive expressions") {
    assertRewrite(
      """MATCH (n)
        |RETURN count(n.val)
      """.stripMargin,
      """MATCH (n)
        |WITH count(n.val) AS `  FRESHID17`
        |RETURN `  FRESHID17` AS `count(n.val)`
      """.stripMargin
    )
  }

  test("introduce WITH clause for aliased non-primitive expressions") {
    assertRewrite(
      """MATCH (n)
        |RETURN count(n.val) AS foo
      """.stripMargin,
      """MATCH (n)
        |WITH count(n.val) AS `  FRESHID17`
        |RETURN `  FRESHID17` AS foo
      """.stripMargin
    )
  }

  test("introduce WITH clause for unaliased primitive and non-primitive expressions") {
    assertRewrite(
      """MATCH (n)
        |RETURN n, n.val, count(n.val)
      """.stripMargin,
      """MATCH (n)
        |WITH n, n.val, count(n.val) AS `  FRESHID27`
        |RETURN n, n.val, `  FRESHID27` AS `count(n.val)`
      """.stripMargin
    )
  }

  test("introduce WITH clause for unaliased, primitive and aliased, non-primitive expressions") {
    assertRewrite(
      """MATCH (n)
        |RETURN n, n.val, count(n.val) AS foo
      """.stripMargin,
      """MATCH (n)
        |WITH n, n.val, count(n.val) AS `  FRESHID27`
        |RETURN n, n.val, `  FRESHID27` AS foo
      """.stripMargin
    )
  }
}
