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
package org.opencypher.caps.impl.parse.rewriter

import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.{CNFNormalizer, Forced, literalReplacement}
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.v3_4.phases.{BaseContext, BaseState, Phase}
import org.neo4j.cypher.internal.util.v3_4.{Rewriter, inSequence}
import org.opencypher.caps.impl.util.MapUtils

case object CAPSRewriting extends Phase[BaseContext, BaseState, BaseState] {

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val term = from.statement()

    val rewrittenStatement = term.endoRewrite(inSequence(
      normalizeReturnClauses(context.exceptionCreator),
      extractSubqueryFromPatternExpression(context.exceptionCreator),
      CNFNormalizer.instance(context)
    ))

    // Extract literals of possibly rewritten subqueries
    // TODO: once this gets into neo4j-frontend, it can be done in literalReplacement
    val (rewriters, extractedParams) = rewrittenStatement.treeFold(Seq.empty[(Rewriter, Map[String, Any])]) {
      case ep: ExistsPattern => acc => (acc :+ literalReplacement(ep.query, Forced), None)
    }.unzip

    // rewrite literals
    val finalStatement = rewriters.foldLeft(rewrittenStatement) {
      case (acc, rewriter) => acc.endoRewrite(rewriter)
    }
    // merge extracted params
    val extractedParameters = extractedParams.foldLeft(Map.empty[String, Any]) {
      case (acc, current) => MapUtils.merge(acc, current)((l, r) => l)
    }

    from.withStatement(finalStatement)
        .withParams(from.extractedParams() ++ extractedParameters)
  }

  override val phase = AST_REWRITE

  override val description = "rewrite the AST into a shape that semantic analysis can be performed on"

  override def postConditions = Set.empty
}
