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

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.{CNFNormalizer, Forced, Namespacer}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.opencypher.caps.impl.CompilationStage

object CypherParser extends CypherParser {
  implicit object defaultContext extends BlankBaseContext
}

trait CypherParser extends CompilationStage[String, Statement, BaseContext] {

  override type Out = (Statement, Map[String, Any])

  override def extract(output: (Statement, Map[String, Any])) = output._1

  override def process(query: String)(implicit context: BaseContext): (Statement, Map[String, Any]) = {
    val startState = BaseStateImpl(query, None, null)
    val endState = pipeLine.transform(startState, context)
    val params = endState.extractedParams
    val rewritten = endState.statement
    rewritten -> params
  }

  protected val pipeLine =
    CompilationPhases.parsing(RewriterStepSequencer.newPlain, Forced) andThen
      SemanticAnalysis(warn = false) andThen
      Namespacer andThen
      CNFNormalizer andThen
      LateAstRewriting andThen sparkCypherRewriting

}

