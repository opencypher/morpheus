package org.opencypher.spark.impl

import org.neo4j.cypher.internal.frontend.v3_2.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.frontend.v3_2.parser.Expressions
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SyntaxException, ast}
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.schema.StdSchema
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.types.{UntypedExpression, SchemaTyper, TypeContext, TypingFailed}
import org.parboiled.scala._

class SchemaTyperTest extends StdTestSuite with AstConstructionTestSupport {

  val schema = StdSchema.empty
    .withNodeKeys("Person")("name" -> CTString, "age" -> CTInteger)
    .withRelationshipKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)
  val typer = SchemaTyper(schema)

  test("typing of simple function") {
    val context = TypeContext.empty
    val expr = parse("timestamp()")

    typer.infer(expr, context) match {
      case result: TypeContext =>
        result.typeTable should contain(expr -> CTInteger)
    }
  }

  test("typing of function with multiple similar signatures") {
    val expr = parse("toInteger(1.0)")

    typer.infer(expr, TypeContext.empty) match {
      case result: TypeContext =>
        result.typeTable should contain(expr -> CTInteger)
    }
  }

  test("typing of function with multiple different signatures") {
    val expr = "size([0, true, []])"

    typer.infer(parse(expr), TypeContext.empty) match {
      case result: TypeContext =>
        result.typeTable should contain(parse(expr) -> CTInteger)
    }
  }

  test("typing of function with multiple signatures with different output types") {
    val intFloatArgs = parse("percentileDisc(1, 3.14)")
    typer.infer(intFloatArgs, TypeContext.empty) match {
      case result: TypeContext =>
        result.typeTable should contain(intFloatArgs -> CTInteger)
    }

    val floatFloatArgs = parse("percentileDisc(6.67, 3.14)")
    typer.infer(floatFloatArgs, TypeContext.empty) match {
      case result: TypeContext =>
        result.typeTable should contain(floatFloatArgs -> CTFloat)
    }

    val numberFloatArgs = parse("percentileDisc([1, 3.14][0], 3.14)")
    typer.infer(numberFloatArgs, TypeContext.empty) match {
      case result: TypeContext =>
        result.typeTable should contain(numberFloatArgs -> CTNumber)
    }
  }

  test("typing of list indexing") {
    val intList = parse("[1, 2][15]")
    typer.infer(intList, TypeContext.empty) match {
      case result: TypeContext =>
        result.typeTable should contain(intList -> CTInteger)
    }

    val numberList = parse("[3.14, -1, 5000][15]")
    typer.infer(numberList, TypeContext.empty) match {
      case result: TypeContext =>
        result.typeTable should contain(numberList -> CTNumber)
    }

    val mixedList = parse("[[], 1, true][15]")
    typer.infer(mixedList, TypeContext.empty) match {
      case result: TypeContext =>
        result.typeTable should contain(mixedList -> CTAny)
    }
  }

  test("infer type of node property lookup") {
    val context = TypeContext.empty.updateType(varFor("n") -> CTNode("Person"))
    val expr = parse("n.name")

    typer.infer(expr, context) match {
      case result: TypeContext =>
        result.typeTable should contain(expr -> CTString)
    }
  }

  test("infer type of relationship property lookup") {
    val context = TypeContext.empty.updateType(varFor("r") -> CTRelationship("KNOWS"))
    val expr = parse("r.relative")

    typer.infer(expr, context) match {
      case result: TypeContext =>
        result.typeTable should contain(prop("r", "relative") -> CTBoolean)
    }
  }

  test("report missing variable") {
    val context = TypeContext.empty
    val expr = parse("r.relative")

    typer.infer(expr, context) match {
      case TypingFailed(errors) => errors should contain(UntypedExpression(varFor("r")))
    }
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

  private case class assertExpr(exprText: String)  {
    def hasType(t: CypherType) = {
      val context = TypeContext.empty
      val expr = parse(exprText)
      typer.infer(expr, context) match {
        case result: TypeContext =>
          result.typeTable should contain(expr -> t)
        case _ =>
          fail(s"Failed to type $exprText")
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
