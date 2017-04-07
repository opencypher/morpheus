package org.opencypher.spark.impl.ir

import org.neo4j.cypher.internal.frontend.v3_2.{ast, symbols}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.api.types.{CTBoolean, CTRelationship, CTWildcard, CypherType}
import org.opencypher.spark.{Neo4jAstTestSupport, StdTestSuite}

class ExpressionConverterTest extends StdTestSuite with Neo4jAstTestSupport {

  test("can convert type() function calls used as predicates") {
    convert(parse("type(r) = 'REL_TYPE'")) should equal(
      HasType(Var("r", CTRelationship), RelTypeRef(0), CTBoolean)
    )
  }

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
    convert(given) should equal(Const(ConstantRef(0)))
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

  test("can convert negation") {
    val given = ast.Not(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _) _

    convert(given) should equal(Not(HasLabel(Var("x"), LabelRef(0))))
  }

  val c = new ExpressionConverter(GlobalsRegistry.none
    .withPropertyKey(PropertyKey("key"))
    .withLabel(Label("Person"))
    .withLabel(Label("Duck"))
    .withPropertyKey(PropertyKey("name"))
    .withConstant(Constant("p"))
    .withRelType(RelType("REL_TYPE"))
  )

  private def typings(e: ast.Expression): CypherType = e match {
    case ast.Variable("r") => CTRelationship
    case _ => CTWildcard
  }

  private def convert(e: ast.Expression): Expr = c.convert(e)(typings)
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


