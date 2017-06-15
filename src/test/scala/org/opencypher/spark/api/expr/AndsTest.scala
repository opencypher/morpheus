package org.opencypher.spark.api.expr

import org.opencypher.spark.TestSuiteImpl
import org.opencypher.spark.api.ir.global.Label

class AndsTest extends TestSuiteImpl {

  test("unnests inner ands") {
    val args: Set[Expr] = Set(Ands(TrueLit()), HasLabel(Var("x")(), Label("X"))(), Ands(Ands(Ands(FalseLit()))))

    Ands(args) should equal(Ands(TrueLit(), HasLabel(Var("x")(), Label("X"))(), FalseLit()))
  }

  test("empty ands not allowed") {
    a [IllegalStateException] should be thrownBy {
      Ands()
    }
  }
}
