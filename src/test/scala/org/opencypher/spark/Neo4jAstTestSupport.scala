package org.opencypher.spark

import org.neo4j.cypher.internal.frontend.v3_2.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_2.parser.Expressions
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SyntaxException, ast}
import org.parboiled.scala.{EOI, Parser}

trait Neo4jAstTestSupport extends AstConstructionTestSupport {

  def parse(exprText: String): ast.Expression = ExpressionParser.parse(exprText, None)

  object ExpressionParser extends Parser with Expressions {

    def Expressions = rule {
      oneOrMore(Expression, separator = WS) ~~ EOI.label("end of input")
    }

    @throws(classOf[SyntaxException])
    def parse(exprText: String, offset: Option[InputPosition]): ast.Expression =
      parseOrThrow(exprText, offset, Expressions)
  }

}
