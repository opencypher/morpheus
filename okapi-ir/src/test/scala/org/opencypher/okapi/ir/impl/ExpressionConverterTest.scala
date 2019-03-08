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
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.test.support.Neo4jAstTestSupport
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing
import org.opencypher.v9_0.ast.semantics.SemanticState
import org.opencypher.v9_0.util.symbols
import org.opencypher.v9_0.{expressions => ast}

import scala.language.implicitConversions

class ExpressionConverterTest extends BaseTestSuite with Neo4jAstTestSupport {

  val baseTypes = Seq[CypherType](
    CTAny, CTNumber, CTNull, CTVoid,
    CTBoolean, CTInteger, CTFloat, CTString,
    CTDate, CTLocalDateTime, CTDuration,
    CTIdentity, CTPath
  )

  val simple =
    baseTypes.map(tpe => tpe.name -> tpe) ++
      baseTypes.map(tpe => s"${tpe.name}_OR_NULL" -> tpe.nullable)

  private val lists = simple
    .map { case (n, t) => s"LIST_$n" -> CTList(t) }

  private val maps = Seq(
    "NODE" -> CTNode(Set("Node")),
    "NODE_EMPTY" -> CTNode(),
    "REL" -> CTRelationship(Set("REL")),
    "REL_EMPTY" -> CTRelationship(),
    "MAP" -> CTMap(simple.toMap),
    "MAP_EMPTY" -> CTMap(Map())
  )

  private val all = Seq(
    simple,
    lists,
    lists.map { case (n, t) => s"${n}_OR_NULL" -> t.nullable },
    maps,
    maps.map { case (n, t) => s"${n}_OR_NULL" -> t.nullable }
  ).flatten.map {
    case (name, typ) => Var(name)(typ)
  }.toSet

  private val properties  =
    simple ++ Seq("name" -> CTString,  "age" -> CTInteger)

  private val properties2 =
    simple ++ Seq("name" -> CTBoolean, "age" -> CTFloat)

  private val propertiesJoined =
    simple ++ Seq("name" -> CTAny, "age" -> CTNumber)

  private val schema: Schema = Schema.empty
    .withNodePropertyKeys("Node")(properties : _*)
    .withRelationshipPropertyKeys("REL")(properties: _*)
    .withNodePropertyKeys("Node2")(properties2 : _*)
    .withRelationshipPropertyKeys("REL2")(properties2: _*)

  val testContext: IRBuilderContext = IRBuilderContext.initial(
    "",
    CypherMap.empty,
    SemanticState.clean,
    IRCatalogGraph(QualifiedGraphName(Namespace(""), GraphName("")), schema),
    qgnGenerator,
    Map.empty,
    _ => ???,
    all
  )

  it("should convert CASE") {
    convert(parseExpr("CASE WHEN INTEGER > INTEGER THEN INTEGER ELSE FLOAT END")) should equal(
      CaseExpr(IndexedSeq((GreaterThan('INTEGER, 'INTEGER), 'INTEGER)), Some('FLOAT))(CTNumber)
    )
    convert(parseExpr("CASE WHEN STRING > STRING_OR_NULL THEN NODE END")) should equal(
      CaseExpr(IndexedSeq((GreaterThan('STRING, 'STRING_OR_NULL), 'NODE)), None)(CTNode("Node").nullable)
    )
  }

  describe("coalesce") {
    it("should convert coalesce") {
      convert(parseExpr("coalesce(INTEGER_OR_NULL, STRING_OR_NULL, NODE)")) should equal(
        Coalesce(IndexedSeq('INTEGER_OR_NULL, 'STRING_OR_NULL, 'NODE))(CTAny)
      )
    }

    it("should become nullable if nothing is non-null") {
      convert(parseExpr("coalesce(INTEGER_OR_NULL, STRING_OR_NULL, NODE_OR_NULL)")) should equal(
        Coalesce(IndexedSeq('INTEGER_OR_NULL, 'STRING_OR_NULL, 'NODE_OR_NULL))(CTAny.nullable)
      )
    }

    it("should not consider arguments past the first non-nullable coalesce") {
      convert(parseExpr("coalesce(INTEGER_OR_NULL, FLOAT, NODE, STRING)")) should equal(
        Coalesce(IndexedSeq('INTEGER_OR_NULL, 'FLOAT))(CTNumber)
      )
    }

    it("should remove coalesce if the first arg is non-nullable") {
      val expr = convert(parseExpr("coalesce(INTEGER, STRING_OR_NULL, NODE)"))
      expr should equal(toVar('INTEGER))
      expr.cypherType should equal(CTInteger)
    }
  }

  describe("exists") {
    // NOTE: pattern version of exists((:A)-->(:B)) is rewritten before IR building

    it("can convert") {
      convert(parseExpr("exists(NODE.key)")) should equal(
        Exists(Property('NODE, PropertyKey("key"))(CTAny))
      )
    }
  }

  test("converting in predicate and literal list") {
    convert(parseExpr("a IN [a, b, c]")) should equal(
      In('a, ListLit('a, 'b, 'c))
    )
  }

  test("converting or predicate") {
    convert(parseExpr("n = a OR n > b")) should equalWithTracing(
      Ors(Equals('n, 'a), GreaterThan('n, 'b))
    )
  }

  test("can convert type") {
    convert(parseExpr("type(a)")) should equal(
      Type(Var("a")())
    )
  }

  it("can convert count") {
    convert(parseExpr("count(a)")) should equal(
      Count(Var("a")(), false)
    )
    convert(parseExpr("count(distinct a)")) should equal(
      Count(Var("a")(), true)
    )
    convert(parseExpr("count(*)")) should equal(
      CountStar
    )
  }

  describe("range") {

    it("can convert range") {
      convert(parseExpr("range(0, 10, 2)")) should equal(
        Range(IntegerLit(0), IntegerLit(10), Some(IntegerLit(2)))
      )
    }

    it("can convert range with missing step size") {
      convert(parseExpr("range(0, 10)")) should equal(
        Range(IntegerLit(0), IntegerLit(10), None)
      )
    }
  }

  describe("substring") {

    it("can convert substring") {
      convert(parseExpr("substring('foobar', 0, 3)")) should equal(
        Substring(StringLit("foobar"), IntegerLit(0), Some(IntegerLit(3)))
      )
    }

    it("can convert substring with missing length") {
      convert(parseExpr("substring('foobar', 0)")) should equal(
        Substring(StringLit("foobar"), IntegerLit(0), None)
      )
    }
  }

  test("can convert less than") {
    convert(parseExpr("a < b")) should equal(
      LessThan(Var("a")(), Var("b")())
    )
  }

  test("can convert less than or equal") {
    convert(parseExpr("a <= b")) should equal(
      LessThanOrEqual(Var("a")(), Var("b")())
    )
  }

  test("can convert greater than") {
    convert(parseExpr("a > b")) should equal(
      GreaterThan(Var("a")(), Var("b")())
    )
  }

  test("can convert greater than or equal") {
    convert(parseExpr("a >= b")) should equal(
      GreaterThanOrEqual(Var("a")(), Var("b")())
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
      HasType(Var("r")(CTRelationship), RelType("REL_TYPE"))
    )
  }

  test("can convert variables") {
    convert(varFor("n")) should equal(('n))
  }

  test("can convert literals") {
    convert(literalInt(1)) should equal(IntegerLit(1L))
    convert(ast.StringLiteral("Hello") _) should equal(StringLit("Hello"))
    convert(parseExpr("false")) should equal(FalseLit)
    convert(parseExpr("true")) should equal(TrueLit)
  }

  test("can convert property access") {
    convert(prop("n", "key")) should equal(Property(('n), PropertyKey("key"))(CTAny))
  }

  test("can convert equals") {
    convert(ast.Equals(varFor("a"), varFor("b")) _) should equal(Equals('a, 'b))
  }

  test("can convert IN for single-element lists") {
    val in = ast.In(varFor("x"), ast.ListLiteral(Seq(ast.StringLiteral("foo") _)) _) _
    convert(in) should equal(Equals('x, StringLit("foo")))
  }

  test("can convert parameters") {
    val given = ast.Parameter("p", symbols.CTString) _
    convert(given) should equal(Param("p")(CTAny))
  }

  test("can convert has-labels") {
    val given = convert(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _, ast.LabelName("Duck") _)) _)
    given should equal(Ands(HasLabel('x, Label("Person")), HasLabel('x, Label("Duck"))))
  }

  test("can convert single has-labels") {
    val given = ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _
    convert(given) should equal(HasLabel('x, Label("Person")))
  }

  test("can convert conjunctions") {
    val given = ast.Ands(
      Set(
        ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _,
        ast.Equals(prop("x", "name"), ast.StringLiteral("Mats") _) _)) _

    convert(given) should equal(
      Ands(
        HasLabel('x, Label("Person")),
        Equals(Property('x, PropertyKey("name"))(CTAny), StringLit("Mats"))))
  }

  test("can convert negation") {
    val given = ast.Not(ast.HasLabels(varFor("x"), Seq(ast.LabelName("Person") _)) _) _

    convert(given) should equal(Not(HasLabel('x, Label("Person"))))
  }

  it("can convert retyping predicate") {
    val given = parseExpr("$p1 AND n:Foo AND $p2 AND m:Bar")

    convert(given).asInstanceOf[Ands].exprs should equalWithTracing(
      Set(HasLabel(('n), Label("Foo")), HasLabel(('m), Label("Bar")), Param("p1")(CTAny), Param("p2")(CTAny)))
  }

  test("can convert id function") {
    convert("id(a)") should equal(
      Id(Var("a")())
    )
  }

  implicit def toVar(s: Symbol): Var = all.find(_.name == s.name).get

  private def convert(e: ast.Expression): Expr =
    new ExpressionConverter(testContext).convert(e)
}
