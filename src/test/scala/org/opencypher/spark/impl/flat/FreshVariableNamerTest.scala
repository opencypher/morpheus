package org.opencypher.spark.impl.flat

import org.opencypher.spark.BaseTestSuite
import org.opencypher.spark.api.types.CTNode

class FreshVariableNamerTest extends BaseTestSuite {

  test("generates prefixed name") {
    FreshVariableNamer("Foobar", CTNode).name should equal("  Foobar")
  }
}
