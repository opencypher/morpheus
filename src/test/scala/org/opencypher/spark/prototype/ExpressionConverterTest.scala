package org.opencypher.spark.prototype

import org.neo4j.cypher.internal.frontend.v3_2.{ast, symbols}
import org.neo4j.cypher.internal.frontend.v3_2.ast.AstConstructionTestSupport
import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.types.CTString
import org.opencypher.spark.prototype.ir._

class ExpressionConverterTest extends StdTestSuite with AstConstructionTestSupport {

  test("can convert variables") {
    convert(varFor("n")) should equal(Var("n"))
  }

  test("can convert literals") {
    convert(literalInt(1)) should equal(IntegerLit(1L))
    convert(ast.StringLiteral("Hello") _) should equal(StringLit("Hello"))
  }

  test("can convert property access") {
    convert(prop("n", "key")) should equal(Property(Var("n"), PropertyKeyRef(0)))
  }

  test("can convert equals") {
    convert(ast.Equals(varFor("a"), varFor("b")) _) should equal(Equals(Var("a"), Var("b")))
  }

  test("can convert IN for single-element lists") {
    val in = ast.In(varFor("x"), ast.ListLiteral(Seq(ast.StringLiteral("foo") _)) _) _
    convert(in) should equal(Equals(Var("x"), StringLit("foo")))
  }

  test("can convert parameters") {
    val given = ast.Parameter("p", symbols.CTString) _
    convert(given) should equal(Param("p"))
  }

  test("can convert has-labels") {
    val given = convert(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _, ast.LabelName("Duck") _)) _)
    given should equal(Ands(HasLabel(Var("x"), LabelRef(0)), HasLabel(Var("x"), LabelRef(1))))
  }

  test("can convert single has-labels") {
    val given = ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _
    convert(given) should equal(HasLabel(Var("x"), LabelRef(0)))
  }

  test("can convert conjunctions") {
    val given = ast.Ands(Set(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _, ast.Equals(prop("x", "name"), ast.StringLiteral("Mats") _) _)) _
    convert(given) should equal(Ands(HasLabel(Var("x"), LabelRef(0)), Equals(Property(Var("x"), PropertyKeyRef(1)), StringLit("Mats"))))
  }

  val c = new ExpressionConverter(TokenRegistry.none
    .withPropertyKey(PropertyKeyDef("key"))
    .withLabel(LabelDef("Person"))
    .withLabel(LabelDef("Duck"))
    .withPropertyKey(PropertyKeyDef("name")))

  private def convert(e: ast.Expression): Expr = c.convert(e)
}
