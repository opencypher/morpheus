package org.opencypher.spark

import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.{CNFNormalizer, Namespacer, Never, rewriteEqualityToInPredicate}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition, SemanticCheckResult, SemanticState}
import org.opencypher.spark.impl.parse.{CypherParser, sparkCypherRewriting}

object CypherParserWithoutSemanticChecking extends CypherParser {
  override val pipeLine =
    Parsing.adds(BaseContains[Statement]) andThen
      SyntaxDeprecationWarnings andThen
      PreparatoryRewriting andThen
      NonThrowingSemanticAnalysis andThen
      AstRewriting(RewriterStepSequencer.newPlain, Never) andThen
      Namespacer andThen
      rewriteEqualityToInPredicate andThen
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
