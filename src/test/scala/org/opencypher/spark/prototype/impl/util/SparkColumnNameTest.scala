package org.opencypher.spark.prototype.impl.util

import org.opencypher.spark.StdTestSuite
import org.opencypher.spark.prototype.impl.spark.SparkColumnName

class SparkColumnNameTest extends StdTestSuite {

  test("escape length 0 spark identifier") {
    fromString("") should equal("_empty_")
  }

  test("escape length 1 spark identifiers") {
    fromString("a") should equal("a")
    fromString("1") should equal("_1")
    fromString("_") should equal("_bar_")
  }

  test("escape length > 1 spark identifiers") {
    fromString("aa") should equal("aa")
    fromString("a1") should equal("a1")
    fromString("_1") should equal("_bar_1")
    fromString("a_") should equal("a_bar_")
  }

  test("escape weird chars") {
    ".?!'\"`=@#$()^&%[]{}<>,:;|+*/\\-".foreach { ch =>
      fromString(s"$ch").forall(esc => Character.isLetter(esc) || esc == '_')
      fromString(s"a$ch").forall(esc => Character.isLetter(esc) || esc == '_')
      fromString(s"1$ch").forall(esc => Character.isLetterOrDigit(esc) || esc == '_')
      fromString(s"_$ch").forall(esc => Character.isLetter(esc) || esc == '_')
    }
  }

  def fromString(text: String) = SparkColumnName.from(Some(text))
}
