package org.opencypher.spark.prototype

import org.neo4j.cypher.internal.frontend.v3_2.parser.{Expressions, Patterns}
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SyntaxException, ast}
import org.opencypher.spark.prototype.api.ir._
import org.opencypher.spark.prototype.api.ir.pattern._
import org.opencypher.spark.prototype.api.ir.global._
import org.opencypher.spark.prototype.impl.convert.PatternConverter
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
        .withEntity('x, EveryNode(AllOf(LabelRef(0))))
        .withEntity('y, EveryNode(AllOf(LabelRef(0), LabelRef(1))))
    )
  }

  test("get rel type") {
    val pattern = parse("(x)-[r:KNOWS | LOVES]->(y)")

    convert(pattern) should equal(
      Pattern.empty
        .withEntity('x, EveryNode)
        .withEntity('y, EveryNode)
        .withEntity('r, EveryRelationship(AnyOf(RelTypeRef(0), RelTypeRef(1))))
        .withConnection('r, DirectedRelationship('x, 'y))
    )
  }

  val converter = new PatternConverter(GlobalsRegistry.none
    .withLabel(Label("Person")).withLabel(Label("Dog"))
    .withRelType(RelType("KNOWS")).withRelType(RelType("LOVES")))

  def convert(p: ast.Pattern) = converter.convert(p)

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
