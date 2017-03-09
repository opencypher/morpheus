package org.opencypher.spark.prototype

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.global.LabelRef

class AndsTest extends StdTestSuite {

  test("unnests inner ands") {
    val args: Set[Expr] = Set(Ands(TrueLit), HasLabel(Var("x"), LabelRef(1)), Ands(Ands(Ands(FalseLit))))

    Ands(args) should equal(Ands(TrueLit, HasLabel(Var("x"), LabelRef(1)), FalseLit))
  }

  test("empty ands not allowed") {
    a [IllegalStateException] should be thrownBy {
      Ands()
    }
  }

}
