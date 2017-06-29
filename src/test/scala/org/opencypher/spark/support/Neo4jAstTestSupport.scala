package org.opencypher.spark.support

import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition, SemanticCheckResult, SemanticState, ast}
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.{CNFNormalizer, Namespacer, Never, rewriteEqualityToInPredicate}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.opencypher.spark.BaseTestSuite
import org.opencypher.spark.impl.parse.{CypherParser, sparkCypherRewriting}

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
}
