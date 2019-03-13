package org.opencypher.spark.impl.acceptance

import org.junit.runner.RunWith
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BigDecimalTests extends CAPSTestSuite with ScanGraphInit {

  describe("big decimal") {

    it("returns a big decimal") {
      caps.cypher("RETURN bigdecimal(1234, 4, 2) AS decimal").records.toMaps should equal(
        Bag(
          CypherMap("decimal" -> BigDecimal("12.34"))
        )
      )
    }

  }

}
