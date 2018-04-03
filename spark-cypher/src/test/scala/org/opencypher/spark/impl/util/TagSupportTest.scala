package org.opencypher.spark.impl.util

import org.opencypher.okapi.test.BaseTestSuite
import org.opencypher.spark.impl.util.TagSupport._

class TagSupportTest extends BaseTestSuite {

  it("computes replacement tags") {
    Set(0).replacementsFor(Set(0)) should equal(Map(0 -> 1))
    Set(0).replacementsFor(Set(1)) should equal(Map(1 -> 1))
    Set(0, 1, 2).replacementsFor(Set(1)) should equal(Map(1 -> 3))
    Set.empty[Int].replacementsFor(Set.empty) should equal(Map.empty)
    Set.empty[Int].replacementsFor(Set(1, 2, 3)) should equal(Map(1 -> 1, 2 -> 2, 3 -> 3))
    Set(1, 2, 3).replacementsFor(Set(1, 2, 3)) should equal(Map(1 -> 4, 2 -> 5, 3 -> 6))
    Set(1, 2, 3).replacementsFor(Set(0, 1)) should equal(Map(0 -> 0, 1 -> 4))
  }

}
