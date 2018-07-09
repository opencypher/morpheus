/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.ir.impl

import org.opencypher.okapi.api.graph.{GraphName, Namespace, QualifiedGraphName}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.impl.util.VarConverters.{toVar, _}
import org.opencypher.okapi.ir.test.support.Neo4jAstTestSupport
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing
import org.opencypher.v9_1.ast.semantics.SemanticState
import org.opencypher.v9_1.util.{Ref, symbols}
import org.opencypher.v9_1.{expressions => ast}

class ExpressionConverterTest extends BaseTestSuite with Neo4jAstTestSupport {

  private def testTypes(ref: Ref[ast.Expression]): CypherType = ref.value match {
    case ast.Variable("r") => AnyRelationship
    case ast.Variable("n") => AnyNode
    case ast.Variable("m") => AnyNode
    case _                 => CTAny
  }

  it("should convert CASE") {
    convert(parseExpr("CASE WHEN a > b THEN c ELSE d END")) should equal(
      CaseExpr(IndexedSeq((GreaterThan('a, 'b)(CTBoolean), 'c)), Some('d))()
    )
    convert(parseExpr("CASE WHEN a > b THEN c END")) should equal(
      CaseExpr(IndexedSeq((GreaterThan('a, 'b)(CTBoolean), 'c)), None)()
    )
  }

  it("should convert coalesce") {
    convert(parseExpr("coalesce(a, b, c)")) should equal(
      Coalesce(IndexedSeq('a, 'b, 'c))()
    )
  }

  test("exists") {
    convert(parseExpr("exists(n.key)")) should equalWithTracing(
      Exists(Property(toNodeVar('n), PropertyKey("key"))())()
    )
  }

  test("converting in predicate and literal list") {
    convert(parseExpr("a IN [a, b, c]")) should equal(
      In('a, ListLit('a, 'b, 'c))()
    )
  }

  test("converting or predicate") {
    convert(parseExpr("n = a OR n > b")) should equalWithTracing(
      Ors(Equals(toNodeVar('n), 'a)(), GreaterThan(toNodeVar('n), 'b)())
    )
  }

  test("can convert type") {
    convert(parseExpr("type(a)")) should equal(
      Type(Var("a")())()
    )
  }

  it("can convert count") {
    convert(parseExpr("count(a)")) should equal(
      Count(Var("a")(), false)()
    )
    convert(parseExpr("count(distinct a)")) should equal(
      Count(Var("a")(), true)()
    )
    convert(parseExpr("count(*)")) should equal(
      CountStar()
    )
  }

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

  test("can convert multiply") {
    convert("a * b") should equal(
      Multiply(Var("a")(), Var("b")())()
    )
  }

  test("can convert divide") {
    convert("a / b") should equal(
      Divide(Var("a")(), Var("b")())()
    )
  }

  test("can convert type function calls used as predicates") {
    convert(parseExpr("type(r) = 'REL_TYPE'")) should equal(
      HasType(Var("r")(AnyRelationship), RelType("REL_TYPE"))(CTBoolean)
    )
  }

  test("can convert variables") {
    convert(varFor("n")) should equal(toNodeVar('n))
  }

  test("can convert literals") {
    convert(literalInt(1)) should equal(IntegerLit(1L)())
    convert(ast.StringLiteral("Hello") _) should equal(StringLit("Hello")())
    convert(parseExpr("false")) should equal(FalseLit)
    convert(parseExpr("true")) should equal(TrueLit)
  }

  test("can convert property access") {
    convert(prop("n", "key")) should equal(Property(toNodeVar('n), PropertyKey("key"))(CTAny))
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
    convert(given) should equal(Param("p")(CTAny))
  }

  test("can convert has-labels") {
    val given = convert(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _, ast.LabelName("Duck") _)) _)
    given should equal(Ands(HasLabel('x, Label("Person"))(CTBoolean), HasLabel('x, Label("Duck"))(CTBoolean)))
  }

  test("can convert single has-labels") {
    val given = ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _
    convert(given) should equal(HasLabel('x, Label("Person"))(CTBoolean))
  }

  test("can convert conjunctions") {
    val given = ast.Ands(
      Set(
        ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _,
        ast.Equals(prop("x", "name"), ast.StringLiteral("Mats") _) _)) _

    convert(given) should equal(
      Ands(
        HasLabel('x, Label("Person"))(CTBoolean),
        Equals(Property('x, PropertyKey("name"))(), StringLit("Mats")())(CTBoolean)))
  }

  test("can convert negation") {
    val given = ast.Not(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _) _

    convert(given) should equal(Not(HasLabel('x, Label("Person"))(CTBoolean))(CTBoolean))
  }

  it("can convert retyping predicate") {
    val given = parseExpr("$p1 AND n:Foo AND $p2 AND m:Bar")

    convert(given).asInstanceOf[Ands].exprs should equalWithTracing(
      Set(HasLabel(toNodeVar('n), Label("Foo"))(), HasLabel(toNodeVar('m), Label("Bar"))(), Param("p1")(), Param("p2")()))
  }

  test("can convert id function") {
    convert("id(a)") should equal(
      Id(Var("a")())()
    )
  }

  lazy val testContext = IRBuilderContext.initial(
    "",
    CypherMap.empty,
    SemanticState.clean,
    IRCatalogGraph(QualifiedGraphName(Namespace(""), GraphName("")), Schema.empty),
    qgnGenerator,
    Map.empty
  )
  private def convert(e: ast.Expression): Expr =
    new ExpressionConverter()(testContext).convert(e)(testTypes)
}
