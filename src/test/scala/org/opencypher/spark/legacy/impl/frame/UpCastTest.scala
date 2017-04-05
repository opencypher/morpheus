package org.opencypher.spark.legacy.impl.frame

import org.opencypher.spark.legacy.api.frame.BinaryRepresentation
import org.opencypher.spark.prototype.api.types.CTMap

class UpCastTest extends StdFrameTestSuite {

  test("Can upcast from NODE to MAP") {
    val n = add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).upcast('n)(_ => CTMap).testResult

      result.signature shouldHaveFields('n -> CTMap)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation)
      result.toSet should equal(Set(n))
    }
  }
}
