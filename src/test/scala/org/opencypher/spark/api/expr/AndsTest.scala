package org.opencypher.spark.api.expr

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.api.ir.global.LabelRef

class AndsTest extends StdTestSuite {

  test("unnests inner ands") {
    val args: Set[Expr] = Set(Ands(TrueLit()), HasLabel(Var("x")(), LabelRef(1))(), Ands(Ands(Ands(FalseLit()))))

    Ands(args) should equal(Ands(TrueLit(), HasLabel(Var("x")(), LabelRef(1))(), FalseLit()))
  }

  test("empty ands not allowed") {
    a [IllegalStateException] should be thrownBy {
      Ands()
    }
  }

}
