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
package org.opencypher.okapi.ir.test.support

import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.testing.BaseTestSuite
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticCheckResult, SemanticState}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.Deprecations.V2
import org.neo4j.cypher.internal.v4_0.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.Never
import org.neo4j.cypher.internal.v4_0.util.{CypherException, InputPosition}

import scala.language.implicitConversions

trait Neo4jAstTestSupport extends AstConstructionTestSupport {

  self: BaseTestSuite =>

  import Neo4jAstTestSupport.CypherParserWithoutSemanticChecking

  def parseQuery(queryText: String): (Statement, Map[String, Any], SemanticState) =
    CypherParserWithoutSemanticChecking.process(queryText)(CypherParser.defaultContext)

  implicit def parseExpr(exprText: String): Expression = {
    CypherParserWithoutSemanticChecking.process(s"RETURN $exprText")(CypherParser.defaultContext)._1 match {
      case Query(_, SingleQuery(Return(_, ReturnItems(_, items), _, _, _, _) :: Nil)) =>
        items.head.expression
      case _ => throw IllegalArgumentException("an expression", exprText)
    }
  }
}

object Neo4jAstTestSupport {

  object CypherParserWithoutSemanticChecking extends CypherParser {
    override val pipeLine: Transformer[BaseContext, BaseState, BaseState] =
      Parsing.adds(BaseContains[Statement]) andThen
        SyntaxDeprecationWarnings(V2) andThen
        OkapiPreparatoryRewriting andThen
        NonThrowingSemanticAnalysis andThen
        AstRewriting(RewriterStepSequencer.newPlain, Never) andThen
        Namespacer andThen
        CNFNormalizer andThen
        LateAstRewriting

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
