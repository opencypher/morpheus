package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast.Pattern
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, SyntaxException, ast}
import org.neo4j.cypher.internal.frontend.v3_2.parser.{Expressions, Patterns}
import org.opencypher.spark.StdTestSuite
import org.parboiled.scala.{EOI, Parser, Rule1}

class PatternConverterTest extends StdTestSuite {

  test("simple node pattern") {
    val pattern = parse("(x)")

    convert(pattern) should equal(Set(AnyNode(Field("x"))))
  }

  test("simple rel pattern") {
    val pattern = parse("(x)-[r]->(b)")

    convert(pattern) should equal(Set(AnyNode(Field("x")), AnyNode(Field("b")), AnyRelationship(Field("r"))))
  }

  test("larger pattern") {
    val pattern = parse("(x)-[r1]->(y)-[r2]->(z)")

    convert(pattern) should equal(Set(
      AnyNode(Field("x")), AnyNode(Field("y")), AnyNode(Field("z")),
      AnyRelationship(Field("r1")), AnyRelationship(Field("r2"))
    ))
  }

  test("disconnected pattern") {
    val pattern = parse("(x), (y)-[r]->(z), (foo)")

    convert(pattern) should equal(Set(
      AnyNode(Field("x")), AnyNode(Field("y")), AnyNode(Field("z")),
      AnyRelationship(Field("r")), AnyNode(Field("foo"))
    ))
  }

  val converter = new PatternConverter(Set(Field("a"), Field("b")))

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
