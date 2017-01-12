package org.opencypher.spark.impl

import org.neo4j.cypher.internal.frontend.v3_2.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_2.parser.Expressions
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SyntaxException, ast}
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.schema.StdSchema
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.types._
import org.parboiled.scala._

class SchemaTyperTest extends StdTestSuite with AstConstructionTestSupport {

  val schema = StdSchema.empty
    .withNodeKeys("Person")("name" -> CTString, "age" -> CTInteger)
    .withRelationshipKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)
  val typer = SchemaTyper(schema)

  test("typing of plus operator") {
    assertExpr("1 + 1") hasType CTInteger
    assertExpr("3.14 + 1") hasType CTFloat
    assertExpr("'foo' + 'bar'") hasType CTString
    assertExpr("[] + [1, 2, 3]") hasType CTList(CTInteger)
    assertExpr("[true] + [1, 2, 3]") hasType CTList(CTAny)

    assertExpr("'foo' + 1") hasType CTString
    assertExpr("'foo' + 3.14") hasType CTString
    assertExpr("'foo' + ['bar', 'giz']") hasType CTList(CTString)

    assertExpr("[] + 1") hasType CTList(CTInteger)
//    assertExpr("[3.14] + 1") hasType CTList(CTNumber)
  }

  test("typing of functions") {
    assertExpr("timestamp()") hasType CTInteger
    assertExpr("toInteger(1.0)") hasType CTInteger
    assertExpr("size([0, true, []])") hasType CTInteger

    assertExpr("percentileDisc(1, 3.14)") hasType CTInteger
    assertExpr("percentileDisc(6.67, 3.14)") hasType CTFloat
    assertExpr("percentileDisc([1, 3.14][0], 3.14)") hasType CTNumber
  }

  test("typing of list indexing") {
    assertExpr("[1, 2][15]") hasType CTInteger
    assertExpr("[3.14, -1, 5000][15]") hasType CTNumber
    assertExpr("[[], 1, true][15]") hasType CTAny
  }

  test("infer type of node property lookup") {
    val context = TypeContext.empty.updateType(varFor("n") -> CTNode("Person"))

    assertExpr("n.name", context) hasType CTString
  }

  test("infer type of relationship property lookup") {
    val context = TypeContext.empty.updateType(varFor("r") -> CTRelationship("KNOWS"))

    assertExpr("r.relative", context) hasType CTBoolean
  }

  test("report untyped expressions") {
    assertExpr("r.relative") hasErrors UntypedExpression(varFor("r"))
  }

  test("basic literals") {
    assertExpr("1") hasType CTInteger
    assertExpr("-3") hasType CTInteger
    assertExpr("true") hasType CTBoolean
    assertExpr("false") hasType CTBoolean
    assertExpr("null") hasType CTNull
    assertExpr("3.14") hasType CTFloat
    assertExpr("-3.14") hasType CTFloat
    assertExpr("'-3.14'") hasType CTString
  }

  test("list literals") {
    assertExpr("[]") hasType CTList(CTVoid)
    assertExpr("[1, 2]") hasType CTList(CTInteger)
    assertExpr("[1, 1.0]") hasType CTList(CTNumber)
    assertExpr("[1, 1.0, '']") hasType CTList(CTAny)
    assertExpr("[1, 1.0, null]") hasType CTList(CTNumber.nullable)
  }

  private case class assertExpr(exprText: String, context: TypeContext = TypeContext.empty)  {
    def hasType(t: CypherType) = {
      val expr = parse(exprText)
      typer.infer(expr, context) match {
        case result: TypeContext =>
          result.typeTable should contain(expr -> t)
        case _ =>
          fail(s"Failed to type $exprText")
      }
    }

    def hasErrors(expected: TypingError*) = {
      val expr = parse(exprText)
      typer.infer(expr, context) match {
        case TypingFailed(actual) =>
          actual should equal(expected.toSeq)
        case _: TypeContext =>
          fail("Expected to get typing errors, but succeeded")
      }
    }
  }

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
