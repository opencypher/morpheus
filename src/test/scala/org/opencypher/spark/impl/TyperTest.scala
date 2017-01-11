package org.opencypher.spark.impl

import org.neo4j.cypher.internal.frontend.v3_2.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_2.parser.Expressions
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SyntaxException, ast}
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.schema.StdSchema
import org.opencypher.spark.api.types._
import org.parboiled.scala._

class TyperTest extends StdTestSuite with AstConstructionTestSupport {

  val schema = StdSchema.empty
    .withNodeKeys("Person")("name" -> CTString, "age" -> CTInteger)
    .withRelationshipKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)
  val typer = Typer(schema)

  test("infer type of node property lookup") {
    val context = TypeContext.empty.updateType(varFor("n") -> CTNode("Person"))
    val expr = ExpressionParser.parse("n.name")

    typer.infer(expr, context) match {
      case result: TypeContext =>
        result.typeTable should equal(Map(
          varFor("n") -> CTNode("Person"),
          prop("n", "name") -> CTString
        ))
    }
  }

  test("infer type of relationship property lookup") {
    val context = TypeContext.empty.updateType(varFor("r") -> CTRelationship("KNOWS"))
    val expr = ExpressionParser.parse("r.relative")

    typer.infer(expr, context) match {
      case result: TypeContext =>
        result.typeTable should contain(prop("r", "relative") -> CTBoolean)
    }
  }

  object ExpressionParser extends Parser with Expressions {

    def Expressions = rule {
      oneOrMore(Expression, separator = WS) ~~ EOI.label("end of input")
    }

    @throws(classOf[SyntaxException])
    def parse(exprText: String, offset: Option[InputPosition] = None): ast.Expression =
      parseOrThrow(exprText, offset, Expressions)
  }
}
