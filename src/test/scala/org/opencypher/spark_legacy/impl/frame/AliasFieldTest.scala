package org.opencypher.spark_legacy.impl.frame

import org.opencypher.spark_legacy.api.frame.BinaryRepresentation
import org.opencypher.spark.api.types.CTNode

class AliasFieldTest extends StdFrameTestSuite {

  test("AliasField renames fields") {
    val a = add(newNode)
    val b = add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.aliasField('n -> 'm).testResult

      result.signature shouldHaveFields('m -> CTNode)
      result.signature shouldHaveFieldSlots('m -> BinaryRepresentation)
      result.toSet should equal(Set(a, b).map(Tuple1(_)))
    }
  }
}
