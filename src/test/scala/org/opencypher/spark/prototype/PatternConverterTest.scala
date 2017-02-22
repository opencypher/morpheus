package org.opencypher.spark.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast.Pattern
import org.neo4j.cypher.internal.frontend.v3_2.parser.{Expressions, Patterns}
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SyntaxException, ast}
import org.opencypher.spark.prototype.ir._
import org.parboiled.scala.{EOI, Parser, Rule1}

import scala.language.implicitConversions

class PatternConverterTest extends IrTestSupport {

  test("simple node pattern") {
    val pattern = parse("(x)")

    convert(pattern) should equal(
      Given.nothing.withEntity('x, AnyNode())
    )
  }

  test("simple rel pattern") {
    val pattern = parse("(x)-[r]->(b)")

    convert(pattern) should equal(
      Given.nothing
        .withEntity('x, AnyNode())
        .withEntity('b, AnyNode())
        .withEntity('r, AnyRelationship())
        .withConnection('r, DirectedRelationship('x, 'b))
    )
  }

  test("larger pattern") {
    val pattern = parse("(x)-[r1]->(y)-[r2]->(z)")

    convert(pattern) should equal(
      Given.nothing
        .withEntity('x, AnyNode())
        .withEntity('y, AnyNode())
        .withEntity('z, AnyNode())
        .withEntity('r1, AnyRelationship())
        .withEntity('r2, AnyRelationship())
        .withConnection('r1, DirectedRelationship('x, 'y))
        .withConnection('r2, DirectedRelationship('y, 'z))
    )
  }

  test("disconnected pattern") {
    val pattern = parse("(x), (y)-[r]->(z), (foo)")

    convert(pattern) should equal(
      Given.nothing
        .withEntity('x, AnyNode())
        .withEntity('y, AnyNode())
        .withEntity('z, AnyNode())
        .withEntity('foo, AnyNode())
        .withEntity('r, AnyRelationship())
        .withConnection('r, DirectedRelationship('y, 'z))
    )
  }

  test("get predicates from undirected pattern") {
    val pattern = parse("(x)-[r]-(y)")

    convert(pattern) should equal(
      Given.nothing
        .withEntity('x, AnyNode())
        .withEntity('y, AnyNode())
        .withEntity('r, AnyRelationship())
        .withConnection('r, UndirectedRelationship('y, 'x))
    )
  }

  test("get labels") {
    val pattern = parse("(x:Person), (y:Dog:Person)")

    convert(pattern) should equal(
      Given.nothing
        .withEntity('x, AnyNode(WithEvery.of(LabelRef(0))))
        .withEntity('y, AnyNode(WithEvery.of(LabelRef(0), LabelRef(1))))
    )
  }

  test("get rel type") {
    val pattern = parse("(x)-[r:KNOWS | LOVES]->(y)")

    convert(pattern) should equal(
      Given.nothing
        .withEntity('x, AnyNode())
        .withEntity('y, AnyNode())
        .withEntity('r, AnyRelationship(WithAny.of(RelTypeRef(0), RelTypeRef(1))))
        .withConnection('r, DirectedRelationship('x, 'y))
    )
  }

  val converter = new PatternConverter(TokenRegistry.none
    .withLabel(LabelDef("Person")).withLabel(LabelDef("Dog"))
    .withRelType(RelTypeDef("KNOWS")).withRelType(RelTypeDef("LOVES")))

  def convert(p: Pattern) = converter.convert(p)

  def parse(exprText: String): ast.Pattern = PatternParser.parse(exprText, None)

  object PatternParser extends Parser with Patterns with Expressions {

    def SinglePattern: Rule1[Seq[Pattern]] = rule {
      oneOrMore(Pattern) ~~ EOI.label("end of input")
    }

    @throws(classOf[SyntaxException])
    def parse(exprText: String, offset: Option[InputPosition]): ast.Pattern =
      parseOrThrow(exprText, offset, SinglePattern)
  }
}
