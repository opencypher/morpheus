package org.opencypher.caps.ir.impl.parse.rewriter

import org.neo4j.cypher.internal.compiler.v3_4.SyntaxExceptionCreator
import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.opencypher.caps.ir.test.support.RewriterTestSupport
import org.opencypher.caps.test.BaseTestSuite

class normalizeCaseExpressionTest extends BaseTestSuite with RewriterTestSupport {
  override val rewriter = normalizeCaseExpression(new SyntaxExceptionCreator("<Query>", Some(InputPosition.NONE)))

  it("should rewrite simple CASE statement") {
    assertRewrite(
      """
        |MATCH (n)
        |RETURN
        |  CASE n.val
        |    WHEN "foo" THEN 1
        |    WHEN "bar" THEN 2
        |  END AS val
      """.stripMargin,
      """
        |MATCH (n)
        |RETURN
        |  CASE
        |    WHEN n.val = "foo" THEN 1
        |    WHEN n.val = "bar" THEN 2
        |  END AS val
      """.stripMargin)
  }

  it("should rewrite simple CASE statement with default") {
    assertRewrite(
      """
        |MATCH (n)
        |RETURN
        |  CASE n.val
        |    WHEN "foo" THEN 1
        |    WHEN "bar" THEN 2
        |    ELSE 3
        |  END AS val
      """.stripMargin,
      """
        |MATCH (n)
        |RETURN
        |  CASE
        |    WHEN n.val = "foo" THEN 1
        |    WHEN n.val = "bar" THEN 2
        |    ELSE 3
        |  END AS val
      """.stripMargin)
  }

  it("should not rewrite generic CASE statement") {
    assertRewrite(
      """
        |MATCH (n)
        |RETURN
        |  CASE
        |    WHEN n.val = "foo" THEN 1
        |    WHEN n.val = "bar" THEN 2
        |    ELSE 3
        |  END AS val
      """.stripMargin,
      """
        |MATCH (n)
        |RETURN
        |  CASE
        |    WHEN n.val = "foo" THEN 1
        |    WHEN n.val = "bar" THEN 2
        |    ELSE 3
        |  END AS val
      """.stripMargin)
  }
}
