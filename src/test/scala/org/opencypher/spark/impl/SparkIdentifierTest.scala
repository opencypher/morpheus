package org.opencypher.spark.impl

import org.opencypher.spark.StdTestSuite

class SparkIdentifierTest extends StdTestSuite {

  test("escape length 0 spark identifier") {
    SparkIdentifier.from("").name should equal("_")
  }

  test("escape length 1 spark identifiers") {
    SparkIdentifier.from("a").name should equal("a")
    SparkIdentifier.from("1").name should equal("_1")
    SparkIdentifier.from("_").name should equal("__")
  }

  test("escape length > 1 spark identifiers") {
    SparkIdentifier.from("aa").name should equal("aa")
    SparkIdentifier.from("a1").name should equal("a1")
    SparkIdentifier.from("_1").name should equal("__1")
    SparkIdentifier.from("a_").name should equal("a__")
  }

  test("escape weird chars") {
    ".?!'\"`=@#$()^&%[]{}<>,:;|+*/\\-".foreach { ch =>
      SparkIdentifier.from(s"$ch").name.forall(esc => Character.isLetter(esc) || esc == '_')
      SparkIdentifier.from(s"a$ch").name.forall(esc => Character.isLetter(esc) || esc == '_')
      SparkIdentifier.from(s"1$ch").name.forall(esc => Character.isLetterOrDigit(esc) || esc == '_')
      SparkIdentifier.from(s"_$ch").name.forall(esc => Character.isLetter(esc) || esc == '_')
    }
  }
}
