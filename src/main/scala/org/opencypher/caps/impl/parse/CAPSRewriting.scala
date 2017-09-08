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
package org.opencypher.caps.impl.parse

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, Rewriter, SemanticCheck, bottomUp}
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.StatementRewriter
import org.neo4j.cypher.internal.frontend.v3_3.phases.{BaseContext, Condition}

object CAPSRewriting extends StatementRewriter {
  override def instance(context: BaseContext): Rewriter = bottomUp(Rewriter.lift {
    case a@Ands(exprs) =>
      val (left, right) = exprs.partition {
        case _: HasLabels => true
        case _ => false
      }
      val singleRight: Expression = right.headOption match {
        case None => True()(a.position)
        case Some(expr) => right.tail.headOption match {
          case None => expr
          case _ => Ands(right)(a.position)
        }
      }
      RetypingPredicate(left, singleRight)(a.position)
  })

  override def description: String = "cos specific rewrites"

  override def postConditions: Set[Condition] = Set.empty
}

case class RetypingPredicate(left: Set[Expression], right: Expression)(val position: InputPosition) extends Expression {
  override def semanticCheck(ctx: Expression.SemanticContext): SemanticCheck =
    Ands(left + right)(position).semanticCheck(ctx)
}
