package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.StdTestSuite

import scala.util.Random

class OrderabilityTest extends StdTestSuite {

  test("should give the correct order") {
    val values: Seq[CypherNumber] = Seq(
      CypherFloat(Double.NegativeInfinity),
      CypherInteger(23L),
      CypherInteger(42L),
      CypherFloat(Double.PositiveInfinity),
      CypherFloat(Double.NaN),
      cypherNull[CypherInteger],
      cypherNull[CypherNumber]
    )

    1.to(100).foreach { _ =>
      Random.shuffle(values).sorted(CypherNumber.equivalence) should equal(values)
    }
  }
}
