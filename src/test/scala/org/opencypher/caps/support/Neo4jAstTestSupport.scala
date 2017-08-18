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
package org.opencypher.caps.support

import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition, SemanticCheckResult, SemanticState, ast}
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.{CNFNormalizer, Namespacer, Never, rewriteEqualityToInPredicate}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.opencypher.caps.BaseTestSuite
import org.opencypher.caps.impl.parse.{CypherParser, sparkCypherRewriting}

import scala.language.implicitConversions

trait Neo4jAstTestSupport extends AstConstructionTestSupport {

  self: BaseTestSuite =>

  import Neo4jAstTestSupport.CypherParserWithoutSemanticChecking

  def parseQuery(queryText: String): (Statement, Map[String, Any]) =
    CypherParser.process(queryText)(CypherParser.defaultContext)

  implicit def parseExpr(exprText: String): ast.Expression = {
    CypherParserWithoutSemanticChecking.process(s"RETURN $exprText")(CypherParser.defaultContext)._1 match {
      case Query(_, SingleQuery(Return(_, ReturnItems(_, items), _, _, _, _) :: Nil)) =>
        items.head.expression
      case _ => throw new IllegalArgumentException(s"This is not an expression, is it: $exprText")
    }
  }
}

object Neo4jAstTestSupport {

  object CypherParserWithoutSemanticChecking extends CypherParser {
    override val pipeLine =
      Parsing.adds(BaseContains[Statement]) andThen
        SyntaxDeprecationWarnings andThen
        PreparatoryRewriting andThen
        NonThrowingSemanticAnalysis andThen
        AstRewriting(RewriterStepSequencer.newPlain, Never) andThen
        Namespacer andThen
        CNFNormalizer andThen
        LateAstRewriting andThen
        sparkCypherRewriting

    object NonThrowingSemanticAnalysis extends SemanticAnalysis(true) {
      override def process(from: BaseState, context: BaseContext): BaseState = {
        val semanticState = NonThrowingChecker.check(from.statement(), context.exceptionCreator)
        from.withSemanticState(semanticState)
      }
    }

    object NonThrowingChecker {
      def check(statement: Statement, mkException: (String, InputPosition) => CypherException): SemanticState = {
        val SemanticCheckResult(semanticState, _) = statement.semanticCheck(SemanticState.clean)
        semanticState
      }
    }
  }
}
