package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast.Pattern
import org.neo4j.cypher.internal.frontend.v3_2.parser.{Expressions, Patterns}
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SyntaxException, ast}
import org.opencypher.spark.StdTestSuite
import org.parboiled.scala.{EOI, Parser, Rule1}

class PatternConverterTest extends StdTestSuite {

  implicit def toField(s: Symbol) = Field(s.name)

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
        .withConnection('r, SimpleConnection('x, 'b))
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
        .withConnection('r1, SimpleConnection('x, 'y))
        .withConnection('r2, SimpleConnection('y, 'z))
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
        .withConnection('r, SimpleConnection('y, 'z))
    )
  }

  test("get predicates from undirected pattern") {
    val pattern = parse("(x)-[r]-(y)")

    convert(pattern) should equal(
      Given.nothing
        .withEntity('x, AnyNode())
        .withEntity('y, AnyNode())
        .withEntity('r, AnyRelationship())
        .withConnection('r, UndirectedConnection('y, 'x))
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
        .withConnection('r, SimpleConnection('x, 'y))
    )
  }

  val converter = new PatternConverter(TokenDefs.none
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
