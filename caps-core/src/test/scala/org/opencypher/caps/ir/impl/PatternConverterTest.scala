/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.caps.ir.impl

import org.neo4j.cypher.internal.frontend.v3_3.InputPosition.NONE
import org.neo4j.cypher.internal.frontend.v3_3.parser.{Expressions, Patterns}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SyntaxException, ast}
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.caps.ir.api.pattern._
import org.opencypher.caps.ir.api.{IRField, Label, RelType}
import org.parboiled.scala.{EOI, Parser, Rule1}

import scala.language.implicitConversions

class PatternConverterTest extends IrTestSuite {

  test("simple node pattern") {
    val pattern = parse("(x)")

    convert(pattern) should equal(
      Pattern.empty.withEntity('x, EveryNode)
    )
  }

  test("simple rel pattern") {
    val pattern = parse("(x)-[r]->(b)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x, EveryNode)
        .withEntity('b, EveryNode)
        .withEntity('r, EveryRelationship)
        .withConnection('r, DirectedRelationship('x, 'b))
    )
  }

  test("larger pattern") {
    val pattern = parse("(x)-[r1]->(y)-[r2]->(z)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x, EveryNode)
        .withEntity('y, EveryNode)
        .withEntity('z, EveryNode)
        .withEntity('r1, EveryRelationship)
        .withEntity('r2, EveryRelationship)
        .withConnection('r1, DirectedRelationship('x, 'y))
        .withConnection('r2, DirectedRelationship('y, 'z))
    )
  }

  test("disconnected pattern") {
    val pattern = parse("(x), (y)-[r]->(z), (foo)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x, EveryNode)
        .withEntity('y, EveryNode)
        .withEntity('z, EveryNode)
        .withEntity('foo, EveryNode)
        .withEntity('r, EveryRelationship)
        .withConnection('r, DirectedRelationship('y, 'z))
    )
  }

  test("get predicates from undirected pattern") {
    val pattern = parse("(x)-[r]-(y)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x, EveryNode)
        .withEntity('y, EveryNode)
        .withEntity('r, EveryRelationship)
        .withConnection('r, UndirectedRelationship('y, 'x))
    )
  }

  test("get labels") {
    val pattern = parse("(x:Person), (y:Dog:Person)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x, EveryNode(AllOf(Label("Person"))))
        .withEntity('y, EveryNode(AllOf(Label("Person"), Label("Dog"))))
    )
  }

  test("get rel type") {
    val pattern = parse("(x)-[r:KNOWS | LOVES]->(y)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x, EveryNode)
        .withEntity('y, EveryNode)
        .withEntity('r, EveryRelationship(AnyOf(RelType("KNOWS"), RelType("LOVES"))))
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
        .withEntity(x, EveryNode)
        .withEntity(y, EveryNode(AllOf(Label("Person"))))
        .withEntity(z, EveryNode)
        .withEntity(r, EveryRelationship)
        .withEntity(newR, EveryRelationship(AnyOf(RelType("IN"))))
        .withConnection(r, DirectedRelationship(x, y))
        .withConnection(newR, DirectedRelationship(y, z))
    )
  }

  val converter = new PatternConverter(Map.empty)

  def convert(p: ast.Pattern, knownTypes: Map[ast.Expression, CypherType] = Map.empty) =
    converter.convert(p, knownTypes)

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
