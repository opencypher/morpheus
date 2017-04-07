package org.opencypher.spark

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.{CNFNormalizer, Namespacer, rewriteEqualityToInPredicate}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.parser.Expressions
import org.neo4j.cypher.internal.frontend.v3_2.phases._
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition, SemanticCheckResult, SemanticState, SyntaxException, ast}
import org.opencypher.spark.impl.parse.CypherParser
import org.parboiled.scala.{EOI, Parser}

trait Neo4jAstTestSupport extends AstConstructionTestSupport {

  def parse(exprText: String): ast.Expression = {
    CypherParserWithoutSemanticChecking.process(s"RETURN $exprText")(CypherParser.defaultContext)._1 match {
      case Query(_, SingleQuery(Return(_, ReturnItems(_, items), _, _, _, _) :: Nil)) =>
        items.head.expression
      case _ => throw new IllegalArgumentException(s"This is not an expression, is it: $exprText")
    }
  }

  object ExpressionParser extends Parser with Expressions {

    def Expressions = rule {
      oneOrMore(Expression, separator = WS) ~~ EOI.label("end of input")
    }

    @throws(classOf[SyntaxException])
    def parse(exprText: String, offset: Option[InputPosition]): ast.Expression =
      parseOrThrow(exprText, offset, Expressions)
  }

}

object CypherParserWithoutSemanticChecking extends CypherParser {
  override val pipeLine =
    Parsing.adds(BaseContains[Statement]) andThen
      SyntaxDeprecationWarnings andThen
      PreparatoryRewriting andThen
      NonThrowingSemanticAnalysis andThen
      AstRewriting(RewriterStepSequencer.newPlain, shouldExtractParams = false) andThen
      Namespacer andThen
      rewriteEqualityToInPredicate andThen
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
