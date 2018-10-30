package org.opencypher.okapi.ir.impl.parse.rewriter

import org.opencypher.okapi.ir.test.support.RewriterTestSupport
import org.opencypher.okapi.testing.BaseTestSuite

class pushLabelsIntoScansTest extends BaseTestSuite with RewriterTestSupport {
  override val rewriter: pushLabelsIntoScans.type = pushLabelsIntoScans

  test("push node label from WHERE clause into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Foo
        |RETURN n
      """.stripMargin,
      """MATCH (n:Foo)
        |RETURN n
      """.stripMargin)
  }

  test("push labels for multiple nodes into pattern") {
    assertRewrite(
      """MATCH (n:Foo)-[:Rel]->(b:Foo:Bar)
        |RETURN n
      """.stripMargin,
      """MATCH (n:Foo)-[:Rel]->(b:Foo:Bar)
        |RETURN n
      """.stripMargin)
  }

  test("keep the node label in the pattern") {
    assertRewrite(
      """MATCH (n:Foo)
        |RETURN n
      """.stripMargin,
      """MATCH (n:Foo)
        |RETURN n
      """.stripMargin)
  }

  test("push additional labels into pattern") {
    assertRewrite(
      """MATCH (n:Foo)
        |WHERE n:Bar
        |RETURN n
      """.stripMargin,
      """MATCH (n:Foo:Bar)
        |RETURN n
      """.stripMargin)
  }

  test("push complex where predicate labels into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Bar AND n:Baz
        |RETURN n
      """.stripMargin,
      """MATCH (n:Bar:Baz)
        |RETURN n
      """.stripMargin)
  }

  test("do not push OR'ed labels into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Bar OR n:Baz
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WHERE n:Bar OR n:Baz
        |RETURN n
      """.stripMargin)
  }

  test("do not push ORS'ed labels into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Foo OR n:Bar OR n:Baz
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WHERE n:Foo OR n:Bar OR n:Baz
        |RETURN n
      """.stripMargin)
  }

  test("do not push NOT'ed labels into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE NOT n:Foo
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WHERE NOT n:Foo
        |RETURN n
      """.stripMargin
    )
  }

  test("do not push XOR'ed labels into pattern") {
    assertRewrite(
      """MATCH (n)
        |WHERE n:Foo XOR n:Bar
        |RETURN n
      """.stripMargin,
      """MATCH (n)
        |WHERE n:Foo XOR n:Bar
        |RETURN n
      """.stripMargin
    )
  }

}
