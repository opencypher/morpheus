package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.api.{Maybe, Ternary, True}
import org.opencypher.spark.impl.newvalue.CypherValue.companion

class CypherValueEqualTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("BOOLEAN equal") {
    verifyEqual(BOOLEAN_valueGroups)
  }

  test("INTEGER equal") {
    verifyEqual(INTEGER_valueGroups)
  }

  test("FLOAT equal") {
    verifyEqual(FLOAT_valueGroups)
  }

  test("NUMBER equal") {
    verifyEqual(NUMBER_valueGroups)
  }

  def verifyEqual[V <: CypherValue : CypherValueCompanion](valueGroups: ValueGroups[V]): Unit = {
    val values = valueGroups.flatten

    values.foreach { v => equal[V](v, v) should be(if (companion[V].containsNull(v)) Maybe else True) }
    values.foreach { v => equal[V](cypherNull, v) should be(Maybe) }
    values.foreach { v => equal[V](v, cypherNull) should be(Maybe) }

    equal[V](cypherNull, cypherNull) should be(Maybe)

    values.foreach { v1 =>
      values.foreach { v2 =>
        if (companion[V].containsNull(v1) || companion[V].containsNull(v2))
          equal[V](v1, v2) should be(Maybe)
        else
          equal[V](v1, v2) should be(Ternary(v1 == v2))
      }
    }
  }

  private def equal[V <: CypherValue : CypherValueCompanion](v1: V, v2: V): Ternary = {
    val cmp1 = companion[V].equal(v1, v2)
    val cmp2 = companion[V].equal(v2, v1)

    cmp1 should equal(cmp2)

    cmp1
  }
}
