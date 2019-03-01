/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherString}
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.impl.util.VarConverters.{toVar, _}
import org.opencypher.okapi.ir.test.support.Neo4jAstTestSupport
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticState
import org.neo4j.cypher.internal.v4_0.util.{Ref, symbols}
import org.neo4j.cypher.internal.v4_0.{expressions => ast}

class ExpressionConverterTest extends BaseTestSuite with Neo4jAstTestSupport {

  private def testTypes(ref: Ref[ast.Expression]): CypherType = ref.value match {
    case ast.Variable("r") => CTRelationship
    case ast.Variable("n") => CTNode
    case ast.Variable("m") => CTNode
    case _                 => CTAny
  }

  it("should convert CASE") {
    convert(parseExpr("CASE WHEN a > b THEN c ELSE d END")) should equal(
      CaseExpr(IndexedSeq((GreaterThan('a, 'b)(CTBoolean), 'c)), Some('d))(CTAny)
    )
    convert(parseExpr("CASE WHEN a > b THEN c END")) should equal(
      CaseExpr(IndexedSeq((GreaterThan('a, 'b)(CTBoolean), 'c)), None)(CTAny)
    )
  }

  it("should convert coalesce") {
    convert(parseExpr("coalesce(a, b, c)")) should equal(
      Coalesce(IndexedSeq('a, 'b, 'c))(CTAny)
    )
  }

  test("exists") {
    convert(parseExpr("exists(n.key)")) should equalWithTracing(
      Exists(Property(toNodeVar('n), PropertyKey("key"))(CTAny))(CTAny)
    )
  }

  test("converting in predicate and literal list") {
    convert(parseExpr("a IN [a, b, c]")) should equal(
      In('a, ListLit('a, 'b, 'c))(CTAny)
    )
  }

  test("converting or predicate") {
    convert(parseExpr("n = a OR n > b")) should equalWithTracing(
      Ors(Equals(toNodeVar('n), 'a)(CTAny), GreaterThan(toNodeVar('n), 'b)(CTAny))
    )
  }

  test("can convert type") {
    convert(parseExpr("type(a)")) should equal(
      Type(Var("a")())(CTAny)
    )
  }

  it("can convert count") {
    convert(parseExpr("count(a)")) should equal(
      Count(Var("a")(), false)(CTAny)
    )
    convert(parseExpr("count(distinct a)")) should equal(
      Count(Var("a")(), true)(CTAny)
    )
    convert(parseExpr("count(*)")) should equal(
      CountStar(CTAny)
    )
  }

  describe("range") {

    it("can convert range") {
      convert(parseExpr("range(0, 10, 2)")) should equal(
        Range(IntegerLit(0)(CTAny), IntegerLit(10)(CTAny), Some(IntegerLit(2)(CTAny)))
      )
    }

    it("can convert range with missing step size") {
      convert(parseExpr("range(0, 10)")) should equal(
        Range(IntegerLit(0)(CTAny), IntegerLit(10)(CTAny), None)
      )
    }
  }

  describe("substring") {

    it("can convert substring") {
      convert(parseExpr("substring('foobar', 0, 3)")) should equal(
        Substring(StringLit("foobar")(CTAny), IntegerLit(0)(CTAny), Some(IntegerLit(3)(CTAny)))
      )
    }

    it("can convert substring with missing length") {
      convert(parseExpr("substring('foobar', 0)")) should equal(
        Substring(StringLit("foobar")(CTAny), IntegerLit(0)(CTAny), None)
      )
    }
  }

  test("can convert less than") {
    convert(parseExpr("a < b")) should equal(
      LessThan(Var("a")(), Var("b")())(CTAny)
    )
  }

  test("can convert less than or equal") {
    convert(parseExpr("a <= b")) should equal(
      LessThanOrEqual(Var("a")(), Var("b")())(CTAny)
    )
  }

  test("can convert greater than") {
    convert(parseExpr("a > b")) should equal(
      GreaterThan(Var("a")(), Var("b")())(CTAny)
    )
  }

  test("can convert greater than or equal") {
    convert(parseExpr("a >= b")) should equal(
      GreaterThanOrEqual(Var("a")(), Var("b")())(CTAny)
    )
  }

  test("can convert add") {
    convert("a + b") should equal(
      Add(Var("a")(), Var("b")())(CTAny)
    )
  }

  test("can convert subtract") {
    convert("a - b") should equal(
      Subtract(Var("a")(), Var("b")())(CTAny)
    )
  }

  test("can convert multiply") {
    convert("a * b") should equal(
      Multiply(Var("a")(), Var("b")())(CTAny)
    )
  }

  test("can convert divide") {
    convert("a / b") should equal(
      Divide(Var("a")(), Var("b")())(CTAny)
    )
  }

  test("can convert type function calls used as predicates") {
    convert(parseExpr("type(r) = 'REL_TYPE'")) should equal(
      HasType(Var("r")(CTRelationship), RelType("REL_TYPE"))(CTBoolean)
    )
  }

  test("can convert variables") {
    convert(varFor("n")) should equal(toNodeVar('n))
  }

  test("can convert literals") {
    convert(literalInt(1)) should equal(IntegerLit(1L)(CTAny))
    convert(ast.StringLiteral("Hello") _) should equal(StringLit("Hello")(CTAny))
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
    convert(in) should equal(Equals('x, StringLit("foo")(CTAny))(CTAny))
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
        Equals(Property('x, PropertyKey("name"))(CTAny), StringLit("Mats")(CTAny))(CTBoolean)))
  }

  test("can convert negation") {
    val given = ast.Not(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _) _

    convert(given) should equal(Not(HasLabel('x, Label("Person"))(CTBoolean))(CTBoolean))
  }

  it("can convert retyping predicate") {
    val given = parseExpr("$p1 AND n:Foo AND $p2 AND m:Bar")

    convert(given).asInstanceOf[Ands].exprs should equalWithTracing(
      Set(HasLabel(toNodeVar('n), Label("Foo"))(CTAny), HasLabel(toNodeVar('m), Label("Bar"))(CTAny), Param("p1")(CTAny), Param("p2")(CTAny)))
  }

  test("can convert id function") {
    convert("id(a)") should equal(
      Id(Var("a")())(CTAny)
    )
  }

  lazy val testContext: IRBuilderContext = IRBuilderContext.initial(
    "",
    CypherMap.empty,
    SemanticState.clean,
    IRCatalogGraph(QualifiedGraphName(Namespace(""), GraphName("")), Schema.empty),
    qgnGenerator,
    Map.empty,
    _ => ???
  )
  private def convert(e: ast.Expression): Expr =
    new ExpressionConverter()(testContext).convert(e)(testTypes)
}
