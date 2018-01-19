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
package org.opencypher.caps.ir.impl.parse

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.frontend.v3_4.semantics._
import org.opencypher.caps.ir.api.exception.ParsingException
import org.opencypher.caps.ir.api.util.CompilationStage
import org.opencypher.caps.ir.impl.parse.rewriter.CAPSRewriting

object CypherParser extends CypherParser {
  implicit object defaultContext extends BlankBaseContext {
    override def errorHandler: (Seq[SemanticErrorDef]) => Unit =
      (errors) => {
        if (errors.nonEmpty) {
          throw ParsingException(s"Errors during semantic checking: ${errors.mkString(", ")}")
        }
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
      AstRewriting(RewriterStepSequencer.newPlain, Forced, getDegreeRewriting = false) andThen
      SemanticAnalysis(warn = false, SemanticFeature.MultipleGraphs, SemanticFeature.WithInitialQuerySignature) andThen
      Namespacer andThen
      CNFNormalizer andThen
      LateAstRewriting andThen
      ExtractPredicatesFromAnds andThen
      CAPSRewriting
}
