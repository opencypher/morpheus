package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.neo4j.cypher.internal.frontend.v3_2.ast.AstConstructionTestSupport
import org.opencypher.spark.StdTestSuite

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

  test("can convert has-labels") {
    val given = convert(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _, ast.LabelName("Duck") _)) _)
    given should equal(Ands(HasLabel(Var("x"), LabelRef(0)), HasLabel(Var("x"), LabelRef(1))))
  }

  val c = new ExpressionConverter(TokenDefs.empty
    .withPropertyKey(PropertyKeyDef("key"))._2
    .withLabel(LabelDef("Person"))._2
    .withLabel(LabelDef("Duck"))._2)

  private def convert(e: ast.Expression): Expr = c.convert(e)
}
