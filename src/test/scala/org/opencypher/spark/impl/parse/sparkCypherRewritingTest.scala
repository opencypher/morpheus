package org.opencypher.spark.impl.parse

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.opencypher.spark.TestSuiteImpl

class sparkCypherRewritingTest extends TestSuiteImpl with AstConstructionTestSupport {

  test("extracts haslabels from ands") {
    val hasLabels = HasLabels(Variable("n") _, Seq(LabelName("name") _))(pos)
    val expr = Ands(Set(hasLabels))(pos)

    val result = sparkCypherRewriting.instance(CypherParser.defaultContext)(expr)

    result should equal(RetypingPredicate(Set(hasLabels), True()(pos))(pos))
  }

  test("extracts only haslabels") {
    val hasLabels = HasLabels(Variable("n") _, Seq(LabelName("name") _))(pos)
    val expr = Ands(Set(hasLabels, False() _))(pos)

    val result = sparkCypherRewriting.instance(CypherParser.defaultContext)(expr)

    result should equal(RetypingPredicate(Set(hasLabels), False()(pos))(pos))
  }

  test("extracts haslabels from inside nested") {
    val hasLabels = HasLabels(Variable("n") _, Seq(LabelName("name") _))(pos)
    val expr = Ors(Set(Ands(Set(hasLabels, False() _))(pos), True() _))(pos)

    val result = sparkCypherRewriting.instance(CypherParser.defaultContext)(expr)

    result should equal(Ors(Set(RetypingPredicate(Set(hasLabels), False()(pos))(pos), True()(pos)))(pos))
  }

  test("extracts all haslabels") {
    val hasLabels1 = HasLabels(Variable("n") _, Seq(LabelName("name") _))(pos)
    val hasLabels2 = HasLabels(Variable("m") _, Seq(LabelName("age") _))(pos)
    val expr = Ands(Set(hasLabels1, False() _, hasLabels2, True() _))(pos)

    val result = sparkCypherRewriting.instance(CypherParser.defaultContext)(expr)

    result should equal(RetypingPredicate(Set(hasLabels1, hasLabels2),
      Ands(Set(False() _, True() _)) _)(pos))
  }

  test("doesn't do anything if no haslabels") {
    val expr = Ands(Set(False() _, True() _))(pos)

    val result = sparkCypherRewriting.instance(CypherParser.defaultContext)(expr)

    result should equal(RetypingPredicate(Set.empty, Ands(Set(False() _, True() _)) _)(pos))
  }

}
