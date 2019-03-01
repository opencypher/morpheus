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

import org.opencypher.okapi.ir.impl.parse.rewriter.legacy.projectFreshSortExpressions
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.{Forced, literalReplacement}
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, inSequence}

case object OkapiLateRewriting extends Phase[BaseContext, BaseState, BaseState] {

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val term = from.statement()

    val rewrittenStatement = term.endoRewrite(
      inSequence(
        projectFreshSortExpressions,
        normalizeCaseExpression,
        normalizeReturnClauses,
        pushLabelsIntoScans,
        extractSubqueryFromPatternExpression(context.exceptionCreator),
        CNFNormalizer.instance(context)
      ))

    // Extract literals of possibly rewritten subqueries
    // TODO: once this gets into neo4j-frontend, it can be done in literalReplacement
    val (rewriters, extractedParams) = rewrittenStatement
      .treeFold(Seq.empty[(Rewriter, Map[String, Any])]) {
        case ep: ExistsPattern =>
          acc =>
            (acc :+ literalReplacement(ep.query, Forced), None)
      }
      .unzip

    // rewrite literals
    val finalStatement = rewriters.foldLeft(rewrittenStatement) {
      case (acc, rewriter) => acc.endoRewrite(rewriter)
    }
    // merge extracted params
    val extractedParameters = extractedParams.foldLeft(Map.empty[String, Any])(_ ++ _)

    from
      .withStatement(finalStatement)
      .withParams(from.extractedParams() ++ extractedParameters)
  }

  override val phase = AST_REWRITE

  override val description = "rewrite the AST into a shape that semantic analysis can be performed on"

  override def postConditions: Set[Condition] = Set.empty
}
