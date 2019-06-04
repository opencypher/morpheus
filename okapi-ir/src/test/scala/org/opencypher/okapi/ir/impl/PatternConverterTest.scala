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

import org.opencypher.okapi.api.graph.{Pattern => _, _}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.ir.api.IRField
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.impl.util.VarConverters.toField
import org.opencypher.v9_0.parser.{Expressions, Patterns}
import org.opencypher.v9_0.util.InputPosition.NONE
import org.opencypher.v9_0.util.{InputPosition, SyntaxException}
import org.opencypher.v9_0.{expressions => ast}
import org.parboiled.scala.{EOI, Parser, Rule1}

import scala.language.implicitConversions

class PatternConverterTest extends IrTestSuite {

  val x = NodeElement("x")(Set.empty)
  val y = NodeElement("y")(Set.empty)
  val z = NodeElement("z")(Set.empty)
  val foo = NodeElement("foo")(Set.empty)
  val r1 = RelationshipElement("r1")(Set.empty)
  val r2 = RelationshipElement("r2")(Set.empty)

  test("simple node pattern") {
    val pattern = parse("(x)")

    convert(pattern) should equal(
      Pattern.empty.withElement(NodeElement("x")(Set.empty))
    )
  }

  it("converts element properties") {
    val pattern = parse("(a:A {name:'Hans'})-[rel:KNOWS {since:2007}]->(a)")
    val a: IRField = 'a -> CTNode("A")
    val rel: IRField = 'rel -> CTRelationship("KNOWS")

    convert(pattern).properties should equal(
      Map(
        a.name -> MapExpression(Map("name" -> StringLit("Hans"))),
        rel.name -> MapExpression(Map("since" -> IntegerLit(2007)))
      )
    )
  }

  test("simple rel pattern") {
    val pattern = parse("(x)-[r1]->(y)")

    convert(pattern) should equal(
      Pattern.empty
        .withElement(x)
        .withElement(y)
        .withElement(r1)
        .withConnection(r1, Connection(Some(x), Some(y), Outgoing))
    )
  }

  test("larger pattern") {
    val pattern = parse("(x)-[r1]->(y)-[r2]->(z)")

    convert(pattern) should equal(
      Pattern.empty
        .withElement(x)
        .withElement(y)
        .withElement(z)
        .withElement(r1)
        .withElement(r2)
        .withConnection(r1, Connection(Some(x), Some(y), Outgoing))
        .withConnection(r2, Connection(Some(y), Some(z), Outgoing))
    )
  }

  test("disconnected pattern") {
    val pattern = parse("(x), (y)-[r1]->(z), (foo)")


    convert(pattern) should equal(
      Pattern.empty
        .withElement(x)
        .withElement(y)
        .withElement(z)
        .withElement(foo)
        .withElement(r1)
        .withConnection(r1, Connection(Some(y), Some(z), Outgoing))
    )
  }

  test("get predicates from undirected pattern") {
    val pattern = parse("(x)-[r1]-(y)")


    convert(pattern) should equal(
      Pattern.empty
        .withElement(x)
        .withElement(y)
        .withElement(r1)
        .withConnection(r1, Connection(Some(x), Some(y), Both))
    )
  }

  test("get labels") {
    val pattern = parse("(x:Person), (y:Dog:Person)")

    val xPerson = NodeElement("x")(Set("Person"))
    val yPersonDog = NodeElement("y")(Set("Person", "Dog"))

    convert(pattern) should equal(
      Pattern.empty
        .withElement(xPerson)
        .withElement(yPersonDog)
    )
  }

  test("get rel type") {
    val pattern = parse("(x)-[r:KNOWS | LOVES]->(y)")

    val rKnowsLoves = RelationshipElement("r")(Set("KNOWS", "LOVES"))

    convert(pattern) should equal(
      Pattern.empty
        .withElement(x)
        .withElement(y)
        .withElement(rKnowsLoves)
        .withConnection(rKnowsLoves, Connection(Some(x), Some(y), Outgoing))
    )
  }

  it("ignores known types") {
    val pattern = parse("(x)-[r1]->(y:Person)-[newR:IN]->(z)")

    val knownTypes: Map[ast.Expression, CypherType] = Map(
      ast.Variable("x")(NONE) -> CTNode("Person"),
      ast.Variable("z")(NONE) -> CTNode("Customer"),
      ast.Variable("r1")(NONE) -> CTRelationship("FOO")
    )

    val yPerson = NodeElement("y")(Set("Person"))
    val newR = RelationshipElement("newR")(Set("IN"))

    convert(pattern, knownTypes) should equal(
      Pattern.empty
        .withElement(x)
        .withElement(yPerson)
        .withElement(z)
        .withElement(r1)
        .withElement(newR)
        .withConnection(r1, Connection(Some(x), Some(yPerson), Outgoing))
        .withConnection(newR, Connection(Some(yPerson), Some(z), Outgoing))
    )
  }

  describe("Conversion of nodes with base element") {
    it("can convert base nodes") {
      val pattern = parse("(y), (x COPY OF y)")

      val knownTypes: Map[ast.Expression, CypherType] = Map(
        ast.Variable("y")(NONE) -> CTNode("Person")
      )

      val xPerson = NodeElement("x")( Set("Person"))
      val yPerson = NodeElement("y")( Set("Person"))

      convert(pattern, knownTypes) should equal(
        Pattern.empty
          .withElement(xPerson)
          .withElement(yPerson)
          .withBaseElement(xPerson, Some(yPerson))
      )
    }

    it("can convert base nodes and add a label") {
      val pattern = parse("(x), (y COPY OF x:Employee)")

      val knownTypes: Map[ast.Expression, CypherType] = Map(
        ast.Variable("x")(NONE) -> CTNode("Person")
      )

      val xPerson = NodeElement("x")( Set("Person"))
      val yPersonEmployee = NodeElement("y")( Set("Person", "Employee"))

      convert(pattern, knownTypes) should equal(
        Pattern.empty
          .withElement(xPerson)
          .withElement(yPersonEmployee)
          .withBaseElement(yPersonEmployee, Some(xPerson))
      )
    }

    it("can convert base relationships") {
      val pattern = parse("(x)-[r1]->(y), (x)-[r2 COPY OF r1]->(y)")

      val knownTypes: Map[ast.Expression, CypherType] = Map(
        ast.Variable("x")(NONE) -> CTNode("Person"),
        ast.Variable("y")(NONE) -> CTNode("Customer"),
        ast.Variable("r1")(NONE) -> CTRelationship("FOO")
      )

      val r1Foo = RelationshipElement("r1")( Set("FOO"))
      val r2Foo = RelationshipElement("r2")( Set("FOO"))

      convert(pattern, knownTypes) should equal(
        Pattern.empty
          .withElement(x)
          .withElement(y)
          .withElement(r1Foo)
          .withElement(r2Foo)
          .withConnection(r1Foo, Connection(Some(x), Some(y), Outgoing))
          .withConnection(r2Foo, Connection(Some(x), Some(y), Outgoing))
          .withBaseElement(r2Foo, Some(r1Foo))
      )
    }

    it("can convert base relationships with new type") {
      val pattern = parse("(x)-[r1]->(y), (x)-[r2 COPY OF r1:BAR]->(y)")

      val knownTypes: Map[ast.Expression, CypherType] = Map(
        ast.Variable("x")(NONE) -> CTNode("Person"),
        ast.Variable("y")(NONE) -> CTNode("Customer"),
        ast.Variable("r1")(NONE) -> CTRelationship("FOO")
      )

      val r1Foo = RelationshipElement("r1")(Set("FOO"))
      val r2FooBar = RelationshipElement("r2")(Set("FOO", "BAR"))

      convert(pattern, knownTypes) should equal(
        Pattern.empty
          .withElement(x)
          .withElement(y)
          .withElement(r1Foo)
          .withElement(r2FooBar)
          .withConnection(r1Foo, Connection(Some(x), Some(y), Outgoing))
          .withConnection(r2FooBar, Connection(Some(x), Some(y), Outgoing))
          .withBaseElement(r2FooBar, Some(r1Foo))
      )
    }
  }

  val converter = new PatternConverter(IRBuilderHelper.emptyIRBuilderContext)

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
