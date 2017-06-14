package org.opencypher.spark

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition, SemanticCheckResult, SemanticState, SyntaxException, ast}
import org.opencypher.spark.impl.parse.{CypherParser, sparkCypherRewriting}

import scala.language.implicitConversions

trait Neo4jAstTestSupport extends AstConstructionTestSupport {

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


