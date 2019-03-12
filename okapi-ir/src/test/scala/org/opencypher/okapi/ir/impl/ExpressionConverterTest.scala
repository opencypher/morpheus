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
import org.opencypher.okapi.ir.test.support.Neo4jAstTestSupport
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.v9_0.ast.semantics.SemanticState
import org.opencypher.v9_0.{expressions => ast}
import org.scalatest.Assertion

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
    CypherMap("p" -> CypherString("myParam")),
    SemanticState.clean,
    IRCatalogGraph(QualifiedGraphName(Namespace(""), GraphName("")), schema),
    qgnGenerator,
    Map.empty,
    _ => ???,
    all
  )

  it("should convert CASE") {
    convert(parseExpr("CASE WHEN INTEGER > INTEGER THEN INTEGER ELSE FLOAT END")) should equal(
      CaseExpr(List((GreaterThan('INTEGER, 'INTEGER), 'INTEGER)), Some('FLOAT))(CTNumber)
    )
    convert(parseExpr("CASE WHEN STRING > STRING_OR_NULL THEN NODE END")) should equal(
      CaseExpr(List((GreaterThan('STRING, 'STRING_OR_NULL), 'NODE)), None)(CTNode("Node").nullable)
    )
  }

  describe("coalesce") {
    it("should convert coalesce") {
      convert(parseExpr("coalesce(INTEGER_OR_NULL, STRING_OR_NULL, NODE)")) shouldEqual
        Coalesce(List('INTEGER_OR_NULL, 'STRING_OR_NULL, 'NODE))(CTAny)
    }

    it("should become nullable if nothing is non-null") {
      convert(parseExpr("coalesce(INTEGER_OR_NULL, STRING_OR_NULL, NODE_OR_NULL)")) shouldEqual
        Coalesce(List('INTEGER_OR_NULL, 'STRING_OR_NULL, 'NODE_OR_NULL))(CTAny.nullable)
    }

    it("should not consider arguments past the first non-nullable coalesce") {
      convert(parseExpr("coalesce(INTEGER_OR_NULL, FLOAT, NODE, STRING)")) shouldEqual
        Coalesce(List('INTEGER_OR_NULL, 'FLOAT))(CTNumber)
    }

    it("should remove coalesce if the first arg is non-nullable") {
      convert(parseExpr("coalesce(INTEGER, STRING_OR_NULL, NODE)")) shouldEqual(
        toVar('INTEGER), CTInteger
      )
    }
  }

  describe("exists") {
    // NOTE: pattern version of exists((:A)-->(:B)) is rewritten before IR building

    it("can convert") {
      convert(parseExpr("exists(NODE.name)")) shouldEqual(
        Exists(Property('NODE, PropertyKey("name"))(CTString)), CTBoolean
      )
    }
  }

  describe("IN") {
    it("can convert in predicate and literal list") {
      convert(parseExpr("INTEGER IN [INTEGER, INTEGER_OR_NULL, FLOAT]")) shouldEqual(
        In('INTEGER, ListLit(List('INTEGER, 'INTEGER_OR_NULL, 'FLOAT))(CTList(CTNumber.nullable))), CTBoolean
      )
    }

    it("can convert IN for single-element lists") {
      convert(parseExpr("STRING IN ['foo']")) shouldEqual(
        Equals('STRING, StringLit("foo")), CTBoolean
      )
    }
  }

  it("can convert or predicate") {
    convert(parseExpr("NODE = NODE_OR_NULL OR STRING_OR_NULL > STRING")) shouldEqual(
      Ors(Equals('NODE, 'NODE_OR_NULL), GreaterThan('STRING_OR_NULL, 'STRING)), CTBoolean.nullable
    )
  }

  describe("type()") {
    it("can convert") {
      convert(parseExpr("type(REL)")) shouldEqual(Type('REL), CTString)
    }

    it("can convert nullable") {
      convert(parseExpr("type(REL_OR_NULL)")) shouldEqual(
        Type('REL_OR_NULL), CTString.nullable
      )
    }
  }

  describe("count()") {
    it("can convert") {
      convert(parseExpr("count(NODE)")) shouldEqual(
        Count('NODE, distinct = false), CTInteger
      )
    }
    it("can convert distinct") {
      convert(parseExpr("count(distinct INTEGER)")) shouldEqual(
        Count('INTEGER, distinct = true), CTInteger
      )
    }
    it("can convert star") {
      convert(parseExpr("count(*)")) shouldEqual(
        CountStar, CTInteger
      )
    }
  }

  describe("range") {

    it("can convert range") {
      convert(parseExpr("range(0, 10, 2)")) shouldEqual(
        Range(IntegerLit(0), IntegerLit(10), Some(IntegerLit(2))), CTList(CTInteger)
      )
    }

    it("can convert range with missing step size") {
      convert(parseExpr("range(0, 10)")) shouldEqual(
        Range(IntegerLit(0), IntegerLit(10), None), CTList(CTInteger)
      )
    }
  }

  describe("substring") {

    it("can convert substring") {
      convert(parseExpr("substring('foobar', 0, 3)")) shouldEqual(
        Substring(StringLit("foobar"), IntegerLit(0), Some(IntegerLit(3))), CTString
      )
    }

    it("can convert substring with missing length") {
      convert(parseExpr("substring('foobar', 0)")) shouldEqual(
        Substring(StringLit("foobar"), IntegerLit(0), None), CTString
      )
    }
  }

  it("can convert less than") {
    convert(parseExpr("INTEGER < FLOAT_OR_NULL")) shouldEqual(
      LessThan('INTEGER, 'FLOAT_OR_NULL), CTBoolean.nullable
    )
  }

  it("can convert less than or equal") {
    convert(parseExpr("INTEGER <= FLOAT_OR_NULL")) shouldEqual(
      LessThanOrEqual('INTEGER, 'FLOAT_OR_NULL), CTBoolean.nullable
    )
  }

  it("can convert greater than") {
    convert(parseExpr("INTEGER > FLOAT_OR_NULL")) shouldEqual(
      GreaterThan('INTEGER, 'FLOAT_OR_NULL), CTBoolean.nullable
    )
  }

  it("can convert greater than or equal") {
    convert(parseExpr("INTEGER >= INTEGER")) shouldEqual(
      GreaterThanOrEqual('INTEGER, 'INTEGER), CTBoolean
    )
  }

  it("can convert add") {
    convert("INTEGER + INTEGER") shouldEqual
      Add('INTEGER, 'INTEGER)(CTInteger)
  }

  it("can convert subtract") {
    convert("INTEGER - INTEGER") shouldEqual
      Subtract('INTEGER, 'INTEGER)(CTInteger)
  }

  it("can convert multiply") {
    convert("FLOAT * INTEGER_OR_NULL") shouldEqual
      Multiply('FLOAT, 'INTEGER_OR_NULL)(CTFloat.nullable)
  }

  it("can convert divide") {
    convert("FLOAT / FLOAT") shouldEqual
      Divide('FLOAT, 'FLOAT)(CTFloat)
  }

  it("can convert type function calls used as predicates") {
    convert(parseExpr("type(REL) = 'REL_TYPE'")) shouldEqual(
      HasType('REL, RelType("REL_TYPE")), CTBoolean
    )
  }

  it("can convert variables") {
    convert(varFor("BOOLEAN")) shouldEqual toVar('BOOLEAN)
  }

  it("can convert literals") {
    convert(literalInt(1)) should equal(IntegerLit(1L))
    convert(ast.StringLiteral("Hello") _) should equal(StringLit("Hello"))
    convert(parseExpr("false")) should equal(FalseLit)
    convert(parseExpr("true")) should equal(TrueLit)
  }

  it("can convert property access") {
    convert(prop("NODE", "age")) shouldEqual
      Property('NODE, PropertyKey("age"))(CTInteger)
  }

  it("can convert equals") {
    convert(ast.Equals(varFor("STRING"), varFor("STRING_OR_NULL")) _) shouldEqual(
      Equals('STRING, 'STRING_OR_NULL), CTBoolean.nullable
    )
  }

  it("can convert parameters") {
    convert(parseExpr("$p")) shouldEqual Param("p")(CTString)
  }

  describe("has labels") {
    it("can convert has-labels") {
      convert(parseExpr("NODE:Person:Duck")) shouldEqual
        Ands(HasLabel('NODE, Label("Person")), HasLabel('NODE, Label("Duck")))
    }

    it("can convert single has-labels") {
      val given = ast.HasLabels(varFor("NODE"), Seq(ast.LabelName("Person") _)) _
      convert(given) shouldEqual(HasLabel('NODE, Label("Person")))
    }
  }

  it("can convert conjunctions") {
    val given = ast.Ands(
      Set(
        ast.HasLabels(varFor("NODE"), Seq(ast.LabelName("Person") _)) _,
        ast.Equals(prop("NODE", "name"), ast.StringLiteral("Mats") _) _)) _

    convert(given) shouldEqual(
      Ands(
        HasLabel('NODE, Label("Person")),
        Equals(Property('NODE, PropertyKey("name"))(CTAny), StringLit("Mats"))), CTBoolean
    )
  }

  it("can convert negation") {
    val given = ast.Not(ast.HasLabels(varFor("NODE"), Seq(ast.LabelName("Person") _)) _) _

    convert(given) shouldEqual(Not(HasLabel('NODE, Label("Person"))))
  }

  it("can convert id function") {
    convert("id(REL_OR_NULL)") shouldEqual(
      Id('REL_OR_NULL), CTIdentity.nullable
    )
  }

  implicit def toVar(s: Symbol): Var = all.find(_.name == s.name).get

  private def convert(e: ast.Expression): Expr =
    new ExpressionConverter(testContext).convert(e)

  implicit class TestExpr(expr: Expr) {
    def shouldEqual(other: Expr): Assertion = {
      expr should equal(other)
      expr.cypherType should equal(other.cypherType)
    }
    def shouldEqual(other: Expr, typ: CypherType): Assertion = {
      expr should equal(other)
      expr.cypherType should equal(other.cypherType)
      expr.cypherType should equal(typ)
    }
  }
}
