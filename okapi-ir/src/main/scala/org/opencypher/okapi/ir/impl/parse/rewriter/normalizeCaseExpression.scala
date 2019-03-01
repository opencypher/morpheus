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

import org.neo4j.cypher.internal.v4_0.expressions.{CaseExpression, Equals}
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, topDown}

/*
  Rewrites special CASE expressions to generic CASE expressions, e.g

  CASE n.val
    WHEN "foo" THEN 1
    WHEN "bar" THEN 2
    ELSE 3
  END

  is rewritten to

  CASE
    WHEN n.val = "foo" THEN 1
    WHEN n.val = "bar" THEN 2
    ELSE 3
  END
 */
case object normalizeCaseExpression extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val instance = topDown(Rewriter.lift {
    case c: CaseExpression => rewriteCase(c)
  })

  private def rewriteCase: CaseExpression => CaseExpression = {
    case expr@CaseExpression(Some(inputExpr), alternatives, default) =>
      val inlineAlternatives = alternatives.map {
        case (predicate, action) => Equals(inputExpr, predicate)(predicate.position) -> action
      }
      CaseExpression(None, inlineAlternatives, default)(expr.position)

    case expr =>
      expr
  }
}
