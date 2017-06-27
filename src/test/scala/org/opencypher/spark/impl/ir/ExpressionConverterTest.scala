package org.opencypher.spark.impl.ir

import org.neo4j.cypher.internal.frontend.v3_2.{Ref, ast, symbols}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.api.types._
import org.opencypher.spark.{Neo4jAstTestSupport, TestSuiteImpl}
import org.opencypher.spark.toVar

class ExpressionConverterTest extends TestSuiteImpl with Neo4jAstTestSupport {

  private val globals = GlobalsRegistry(
    TokenRegistry
    .empty
    .withPropertyKey(PropertyKey("key"))
    .withLabel(Label("Person"))
    .withLabel(Label("Duck"))
    .withLabel(Label("Foo"))
    .withLabel(Label("Bar"))
    .withPropertyKey(PropertyKey("name"))
    .withRelType(RelType("REL_TYPE")),
    ConstantRegistry
    .empty
    .withConstant(Constant("p"))
    .withConstant(Constant("p1"))
    .withConstant(Constant("p2"))
  )

  private def testTypes(ref: Ref[ast.Expression]): CypherType = ref.value match {
    case ast.Variable("r") => CTRelationship
    case ast.Variable("n") => CTNode
    case ast.Variable("m") => CTNode
    case _ => CTWildcard
  }

  import globals.tokens._
  import globals.constants._

  private val c = new ExpressionConverter(globals)

  test("can convert less than") {
    convert(parseExpr("a < b")) should equal(
      LessThan(Var("a")(), Var("b")())()
    )
  }

  test("can convert less than or equal") {
    convert(parseExpr("a <= b")) should equal(
      LessThanOrEqual(Var("a")(), Var("b")())()
    )
  }

  test("can convert greater than") {
    convert(parseExpr("a > b")) should equal(
      GreaterThan(Var("a")(), Var("b")())()
    )
  }

  test("can convert greater than or equal") {
    convert(parseExpr("a >= b")) should equal(
      GreaterThanOrEqual(Var("a")(), Var("b")())()
    )
  }

  test("can convert add") {
    convert("a + b") should equal(
      Add(Var("a")(), Var("b")())()
    )
  }

  test("can convert subtract") {
    convert("a - b") should equal(
      Subtract(Var("a")(), Var("b")())()
    )
  }

  test("can convert type() function calls used as predicates") {
    convert(parseExpr("type(r) = 'REL_TYPE'")) should equal(
      HasType(Var("r")(CTRelationship), RelType("REL_TYPE"))(CTBoolean)
    )
  }

  test("can convert variables") {
    convert(varFor("n")) should equal(toVar('n))
  }

  test("can convert literals") {
    convert(literalInt(1)) should equal(IntegerLit(1L)())
    convert(ast.StringLiteral("Hello") _) should equal(StringLit("Hello")())
    convert(parseExpr("false")) should equal(FalseLit())
    convert(parseExpr("true")) should equal(TrueLit())
  }

  test("can convert property access") {
    convert(prop("n", "key")) should equal(Property('n, PropertyKey("key"))(CTWildcard))
  }

  test("can convert equals") {
    convert(ast.Equals(varFor("a"), varFor("b")) _) should equal(Equals('a, 'b)(CTBoolean))
  }

  test("can convert IN for single-element lists") {
    val in = ast.In(varFor("x"), ast.ListLiteral(Seq(ast.StringLiteral("foo") _)) _) _
    convert(in) should equal(Equals('x, StringLit("foo")())())
  }

  test("can convert parameters") {
    val given = ast.Parameter("p", symbols.CTString) _
    convert(given) should equal(Const(Constant("p"))(CTWildcard))
  }

  test("can convert has-labels") {
    val given = convert(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _, ast.LabelName("Duck") _)) _)
    given should equal(Ands(HasLabel('x, labelByName("Person"))(CTBoolean), HasLabel('x, Label("Duck"))(CTBoolean)))
  }

  test("can convert single has-labels") {
    val given = ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _
    convert(given) should equal(HasLabel('x, labelByName("Person"))(CTBoolean))
  }

  test("can convert conjunctions") {
    val given = ast.Ands(Set(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _, ast.Equals(prop("x", "name"), ast.StringLiteral("Mats") _) _)) _

    convert(given) should equal(Ands(HasLabel('x, labelByName("Person"))(CTBoolean), Equals(Property('x, PropertyKey("name"))(), StringLit("Mats")())(CTBoolean)))
  }

  test("can convert negation") {
    val given = ast.Not(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _) _

    convert(given) should equal(Not(HasLabel('x, labelByName("Person"))(CTBoolean))(CTBoolean))
  }

  test("can convert retyping predicate") {
    val given = parseExpr("$p1 AND n:Foo AND $p2 AND m:Bar")

    convert(given) should equal(Ands(
      HasLabel('n, labelByName("Foo"))(),
      HasLabel('m, labelByName("Bar"))(),
      Const(constantByName("p1"))(),
      Const(constantByName("p2"))())
    )
  }

  private def convert(e: ast.Expression): Expr = c.convert(e)(testTypes)
}


/*
 n => n:Person

 MATCH (n) WHERE n:Person, ...
 |
 v
 n:Person


 (n) -> Var(n)
 (n) -> n:Person n.prop, n.ddd


 Expand(n-[r]->m)-----|
 |                    |
 Filter(n:Person)     Filter(r, "ATTENDED")
 |
 NodeScan(n, n.prop1, n.prop2, ....)

 (1) Stage 1
 (2) Use-analysis



 LabelScan(n:Person) -> LabelScan(n:Person(name, age))
 */


