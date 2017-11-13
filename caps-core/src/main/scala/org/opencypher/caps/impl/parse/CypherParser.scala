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
package org.opencypher.caps.impl.parse

import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.neo4j.cypher.internal.frontend.v3_3.{SemanticError, SemanticErrorDef, SemanticFeature, SemanticState}
import org.opencypher.caps.impl.CompilationStage
import org.opencypher.caps.impl.parse.rewriter.CAPSRewriting
import org.opencypher.caps.impl.spark.exception.Raise

object CypherParser extends CypherParser {
  implicit object defaultContext extends BlankBaseContext {
    override def errorHandler: (Seq[SemanticErrorDef]) => Unit =
      (errors) => {
        val filtered = errors.filter {
          // TODO: Fix by updating frontend dependency
          // Related to using bound variables in GRAPH OF
          case s: SemanticError if s.msg.matches("Variable .* already declared") => false
          case _                                                                 => true
        }
        Raise.semanticErrors(filtered)
      }
  }
}

trait CypherParser extends CompilationStage[String, Statement, BaseContext] {

  override type Out = (Statement, Map[String, Any], SemanticState)

  override def extract(output: (Statement, Map[String, Any], SemanticState)): Statement = output._1

  override def process(query: String)(implicit context: BaseContext): (Statement, Map[String, Any], SemanticState) = {
    val startState = BaseStateImpl(query, None, null)
    val endState = pipeLine.transform(startState, context)
    val params = endState.extractedParams
    val rewritten = endState.statement
    (rewritten, params, endState.maybeSemantics.get)
  }

  protected val pipeLine: Transformer[BaseContext, BaseState, BaseState] =
    Parsing.adds(BaseContains[Statement]) andThen
      SyntaxDeprecationWarnings andThen
      PreparatoryRewriting andThen
      SemanticAnalysis(warn = true, SemanticFeature.MultipleGraphs, SemanticFeature.WithInitialQuerySignature)
        .adds(BaseContains[SemanticState]) andThen
      AstRewriting(RewriterStepSequencer.newPlain, Forced) andThen
      SemanticAnalysis(warn = false, SemanticFeature.MultipleGraphs, SemanticFeature.WithInitialQuerySignature) andThen
      Namespacer andThen
      CNFNormalizer andThen
      LateAstRewriting andThen
      ExtractPredicatesFromAnds andThen
      CAPSRewriting
}
