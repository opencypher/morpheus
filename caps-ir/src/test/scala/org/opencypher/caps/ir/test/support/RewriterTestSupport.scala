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
package org.opencypher.caps.ir.test.support

import org.neo4j.cypher.internal.frontend.v3_4.ast.{AstConstructionTestSupport, Statement}
import org.neo4j.cypher.internal.frontend.v3_4.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticChecker
import org.neo4j.cypher.internal.util.v3_4.Rewriter
import org.opencypher.caps.test.BaseTestSuite

import scala.language.implicitConversions

trait RewriterTestSupport extends AstConstructionTestSupport {

  self: BaseTestSuite =>

  def rewriter: Rewriter

  private val parser = new CypherParser

  def assertRewrite(originalQuery: String, expectedQuery: String): Unit = {
    val original = parseForRewriting(originalQuery)
    val expected = parseForRewriting(expectedQuery)
    SemanticChecker.check(original)
    val result = rewrite(original)
    assert(result === expected, "\n" + originalQuery)
  }

  private def parseForRewriting(queryText: String): Statement = parser.parse(queryText.replace("\r\n", "\n"))

  private def rewrite(original: Statement): AnyRef =
    original.rewrite(rewriter)

}


