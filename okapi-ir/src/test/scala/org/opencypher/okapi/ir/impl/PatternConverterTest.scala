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

import org.opencypher.okapi.api.types.{CTAny, CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.ir.api.IRField
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.impl.util.VarConverters.toField
import org.neo4j.cypher.internal.v4_0.parser.{Expressions, Patterns}
import org.neo4j.cypher.internal.v4_0.util.InputPosition.NONE
import org.neo4j.cypher.internal.v4_0.util.{InputPosition, SyntaxException}
import org.neo4j.cypher.internal.v4_0.{expressions => ast}
import org.parboiled.scala.{EOI, Parser, Rule1}

import scala.language.implicitConversions

class PatternConverterTest extends IrTestSuite {

  test("simple node pattern") {
    val pattern = parse("(x)")

    convert(pattern) should equal(
      Pattern.empty.withEntity('x -> CTNode)
    )
  }

  it("converts entity properties") {
    val pattern = parse("(a:A {name:'Hans'})-[rel:KNOWS {since:2007}]->(a)")
    val a: IRField = 'a -> CTNode("A")
    val rel: IRField = 'rel -> CTRelationship("KNOWS")

    convert(pattern).properties should equal(
      Map(
        a -> MapExpression(Map("name" -> StringLit("Hans")()))(CTAny),
        rel -> MapExpression(Map("since" -> IntegerLit(2007)()))(CTAny)
      )
    )
  }

  test("simple rel pattern") {
    val pattern = parse("(x)-[r]->(b)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x -> CTNode)
        .withEntity('b -> CTNode)
        .withEntity('r -> CTRelationship)
        .withConnection('r, DirectedRelationship('x, 'b))
    )
  }

  test("larger pattern") {
    val pattern = parse("(x)-[r1]->(y)-[r2]->(z)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x -> CTNode)
        .withEntity('y -> CTNode)
        .withEntity('z -> CTNode)
        .withEntity('r1 -> CTRelationship)
        .withEntity('r2 -> CTRelationship)
        .withConnection('r1, DirectedRelationship('x, 'y))
        .withConnection('r2, DirectedRelationship('y, 'z))
    )
  }

  test("disconnected pattern") {
    val pattern = parse("(x), (y)-[r]->(z), (foo)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x -> CTNode)
        .withEntity('y -> CTNode)
        .withEntity('z -> CTNode)
        .withEntity('foo -> CTNode)
        .withEntity('r -> CTRelationship)
        .withConnection('r, DirectedRelationship('y, 'z))
    )
  }

  test("get predicates from undirected pattern") {
    val pattern = parse("(x)-[r]-(y)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x -> CTNode)
        .withEntity('y -> CTNode)
        .withEntity('r -> CTRelationship)
        .withConnection('r, UndirectedRelationship('y, 'x))
    )
  }

  test("get labels") {
    val pattern = parse("(x:Person), (y:Dog:Person)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x -> CTNode("Person"))
        .withEntity('y -> CTNode("Person", "Dog"))
    )
  }

  test("get rel type") {
    val pattern = parse("(x)-[r:KNOWS | LOVES]->(y)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x -> CTNode)
        .withEntity('y -> CTNode)
        .withEntity('r -> CTRelationship("KNOWS", "LOVES"))
        .withConnection('r, DirectedRelationship('x, 'y))
    )
  }

  test("reads type from knownTypes") {
    val pattern = parse("(x)-[r]->(y:Person)-[newR:IN]->(z)")

    val knownTypes: Map[ast.Expression, CypherType] = Map(
      ast.Variable("x")(NONE) -> CTNode("Person"),
      ast.Variable("z")(NONE) -> CTNode("Customer"),
      ast.Variable("r")(NONE) -> CTRelationship("FOO")
    )

    val x: IRField = 'x -> CTNode("Person")
    val y: IRField = 'y -> CTNode("Person")
    val z: IRField = 'z -> CTNode("Customer")
    val r: IRField = 'r -> CTRelationship("FOO")
    val newR: IRField = 'newR -> CTRelationship("IN")

    convert(pattern, knownTypes) should equal(
      Pattern.empty
        .withEntity(x)
        .withEntity(y)
        .withEntity(z)
        .withEntity(r)
        .withEntity(newR)
        .withConnection(r, DirectedRelationship(x, y))
        .withConnection(newR, DirectedRelationship(y, z))
    )
  }

  describe("Conversion of nodes with base entity") {
    it("can convert base nodes") {
      val pattern = parse("(y), (x COPY OF y)")

      val knownTypes: Map[ast.Expression, CypherType] = Map(
        ast.Variable("y")(NONE) -> CTNode("Person")
      )

      val x: IRField = 'x -> CTNode("Person")
      val y: IRField = 'y -> CTNode("Person")

      convert(pattern, knownTypes) should equal(
        Pattern.empty
          .withEntity(x)
          .withEntity(y)
          .withBaseField(x, Some(y))
      )
    }

    it("can convert base nodes and add a label") {
      val pattern = parse("(x), (y COPY OF x:Employee)")

      val knownTypes: Map[ast.Expression, CypherType] = Map(
        ast.Variable("x")(NONE) -> CTNode("Person")
      )

      val x: IRField = 'x -> CTNode("Person")
      val y: IRField = 'y -> CTNode("Person", "Employee")

      convert(pattern, knownTypes) should equal(
        Pattern.empty
          .withEntity(x)
          .withEntity(y)
          .withBaseField(y, Some(x))
      )
    }

    it("can convert base relationships") {
      val pattern = parse("(x)-[r]->(y), (x)-[r2 COPY OF r]->(y)")

      val knownTypes: Map[ast.Expression, CypherType] = Map(
        ast.Variable("x")(NONE) -> CTNode("Person"),
        ast.Variable("y")(NONE) -> CTNode("Customer"),
        ast.Variable("r")(NONE) -> CTRelationship("FOO")
      )

      val x: IRField = 'x -> CTNode("Person")
      val y: IRField = 'y -> CTNode("Person")
      val r: IRField = 'r -> CTRelationship("FOO")
      val r2: IRField = 'r2 -> CTRelationship("FOO")

      convert(pattern, knownTypes) should equal(
        Pattern.empty
          .withEntity(x)
          .withEntity(y)
          .withEntity(r)
          .withEntity(r2)
          .withConnection(r, DirectedRelationship(x, y))
          .withConnection(r2, DirectedRelationship(x, y))
          .withBaseField(r2, Some(r))
      )
    }

    it("can convert base relationships with new type") {
      val pattern = parse("(x)-[r]->(y), (x)-[r2 COPY OF r:BAR]->(y)")

      val knownTypes: Map[ast.Expression, CypherType] = Map(
        ast.Variable("x")(NONE) -> CTNode("Person"),
        ast.Variable("y")(NONE) -> CTNode("Customer"),
        ast.Variable("r")(NONE) -> CTRelationship("FOO")
      )

      val x: IRField = 'x -> CTNode("Person")
      val y: IRField = 'y -> CTNode("Person")
      val r: IRField = 'r -> CTRelationship("FOO")
      val r2: IRField = 'r2 -> CTRelationship("BAR")

      convert(pattern, knownTypes) should equal(
        Pattern.empty
          .withEntity(x)
          .withEntity(y)
          .withEntity(r)
          .withEntity(r2)
          .withConnection(r, DirectedRelationship(x, y))
          .withConnection(r2, DirectedRelationship(x, y))
          .withBaseField(r2, Some(r))
      )
    }
  }
  val converter = new PatternConverter()(IRBuilderHelper.emptyIRBuilderContext)

  def convert(p: ast.Pattern, knownTypes: Map[ast.Expression, CypherType] = Map.empty): Pattern =
    converter.convert(p, knownTypes, testQualifiedGraphName)

  def parse(exprText: String): ast.Pattern = PatternParser.parse(exprText, None)

  object PatternParser extends Parser with Patterns with Expressions {

    def SinglePattern: Rule1[Seq[ast.Pattern]] = rule {
      oneOrMore(Pattern) ~~ EOI.label("end of input")
    }

    @throws(classOf[SyntaxException])
    def parse(exprText: String, offset: Option[InputPosition]): ast.Pattern =
      parseOrThrow(exprText, offset, SinglePattern)
  }

}
