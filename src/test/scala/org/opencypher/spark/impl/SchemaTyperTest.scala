package org.opencypher.spark.impl

import cats.data.NonEmptyList
import org.neo4j.cypher.internal.frontend.v3_2.ast.{AstConstructionTestSupport, Expression, Parameter}
import org.neo4j.cypher.internal.frontend.v3_2.parser.Expressions
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SyntaxException, ast, symbols}
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.schema.StdSchema
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.types._
import org.parboiled.scala._
import org.scalatest.mockito.MockitoSugar

class SchemaTyperTest extends StdTestSuite with AstConstructionTestSupport with MockitoSugar {

  val schema = StdSchema.empty
    .withNodeKeys("Person")("name" -> CTString, "age" -> CTInteger)
    .withRelationshipKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)

  val typer = SchemaTyper(schema)

  test("typing of unsupported expressions") {
    val expr = mock[Expression]
    assertExpr(expr) shouldFailToInferTypeWithErrors UnsupportedExpr(expr)
  }

  test("typing of variables") {
    implicit val context = TyperContext.empty :+ varFor("n") -> CTNode("Person")

    assertExpr.from("n") shouldHaveInferredType CTNode("Person")
  }

  test("typing of parameters (1)") {
    implicit val context = TyperContext.empty :+ Parameter("param", symbols.CTAny)(pos) -> CTNode("Person")

    assertExpr.from("$param") shouldHaveInferredType CTNode("Person")
  }

  test("typing of parameters (2)") {
    implicit val context = TyperContext.empty :+ Parameter("param", symbols.CTNode)(pos) -> CTAny

    assertExpr.from("$param") shouldHaveInferredType CTNode
  }

  test("typing of basic literals") {
    assertExpr.from("1") shouldHaveInferredType CTInteger
    assertExpr.from("-3") shouldHaveInferredType CTInteger
    assertExpr.from("true") shouldHaveInferredType CTBoolean
    assertExpr.from("false") shouldHaveInferredType CTBoolean
    assertExpr.from("null") shouldHaveInferredType CTNull
    assertExpr.from("3.14") shouldHaveInferredType CTFloat
    assertExpr.from("-3.14") shouldHaveInferredType CTFloat
    assertExpr.from("'-3.14'") shouldHaveInferredType CTString
  }

  test("typing of list literals") {
    assertExpr.from("[]") shouldHaveInferredType CTList(CTVoid)
    assertExpr.from("[1, 2]") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("[1, 1.0]") shouldHaveInferredType CTList(CTNumber)
    assertExpr.from("[1, 1.0, '']") shouldHaveInferredType CTList(CTAny)
    assertExpr.from("[1, 1.0, null]") shouldHaveInferredType CTList(CTNumber.nullable)
  }

  test("typing of list indexing") {
    assertExpr.from("[1, 2][15]") shouldHaveInferredType CTVoid
    assertExpr.from("[3.14, -1, 5000][15]") shouldHaveInferredType CTVoid
    assertExpr.from("[[], 1, true][15]") shouldHaveInferredType CTVoid

    assertExpr.from("[1, 2][1]") shouldHaveInferredType CTInteger

    implicit val context = TyperContext.empty :+ Parameter("param", symbols.CTAny)(pos) -> CTInteger

    assertExpr.from("[3.14, -1, 5000][$param]") shouldHaveInferredType CTNumber
    assertExpr.from("[[], 1, true][$param]") shouldHaveInferredType CTAny
  }

  test("infer type of node property lookup") {
    implicit val context = TyperContext(Map(varFor("n") -> CTNode("Person")))

    assertExpr.from("n.name") shouldHaveInferredType CTString
  }

  test("infer type of relationship property lookup") {
    implicit val context = TyperContext(Map(varFor("r") -> CTRelationship("KNOWS")))

    assertExpr.from("r.relative") shouldHaveInferredType CTBoolean
  }

  test("typing of plus operator") {
    assertExpr.from("1 + 1") shouldHaveInferredType CTInteger
    assertExpr.from("3.14 + 1") shouldHaveInferredType CTFloat
    assertExpr.from("'foo' + 'bar'") shouldHaveInferredType CTString
    assertExpr.from("[] + [1, 2, 3]") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("[true] + [1, 2, 3]") shouldHaveInferredType CTList(CTAny)

    assertExpr.from("'foo' + 1") shouldHaveInferredType CTString
    assertExpr.from("'foo' + 3.14") shouldHaveInferredType CTString
    assertExpr.from("'foo' + ['bar', 'giz']") shouldHaveInferredType CTList(CTString)

    assertExpr.from("[] + 1") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("[3.14] + 1") shouldHaveInferredType CTList(CTNumber)
  }

  test("typing of functions") {
    assertExpr.from("timestamp()") shouldHaveInferredType CTInteger
    assertExpr.from("toInteger(1.0)") shouldHaveInferredType CTInteger
    assertExpr.from("size([0, true, []])") shouldHaveInferredType CTInteger

    assertExpr.from("percentileDisc(1, 3.14)") shouldHaveInferredType CTInteger
    assertExpr.from("percentileDisc(6.67, 3.14)") shouldHaveInferredType CTFloat
    assertExpr.from("percentileDisc([1, 3.14][0], 3.14)") shouldHaveInferredType CTInteger

    // TODO: Making this work requires union types and changing how nullability works. Sad!
    //
    // implicit val context = TyperContext.empty :+ Parameter("param", symbols.CTAny)(pos) -> CTInteger
    // assertExpr.from("percentileDisc([1, 3.14][$param], 3.14)") shouldHaveInferredType CTInteger
  }

  private object assertExpr {
    def from(exprText: String)(implicit context: TyperContext = TyperContext.empty) =
      assertExpr(parse(exprText))
  }

  private case class assertExpr(expr: Expression)(implicit val context: TyperContext = TyperContext.empty)  {

    def shouldHaveInferredType(expected: CypherType) = {
      val actual = typer.inferOrThrow(expr, context).context.typings.get(expr)
      actual shouldBe Some(expected)
    }

    def shouldFailToInferTypeWithErrors(expectedHd: TyperError, expectedTail: TyperError*) = {
      typer.infer(expr, context) match {
        case Left(actual) =>
          actual.toList.toSet should equal(NonEmptyList.of(expectedHd, expectedTail: _*).toList.toSet)
        case _ =>
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
