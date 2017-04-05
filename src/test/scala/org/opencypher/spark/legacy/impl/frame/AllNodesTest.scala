package org.opencypher.spark.legacy.impl.frame

import org.opencypher.spark.legacy.api.frame.BinaryRepresentation
import org.opencypher.spark.prototype.api.types.CTNode

class AllNodesTest extends StdFrameTestSuite {

  test("AllNodes produces all input nodes") {
    val n1 = add(newNode.withLabels("A").withProperties("name" -> "Zippie"))
    val n2 = add(newNode.withLabels("B").withProperties("name" -> "Yggie"))

    new GraphTest {
      val result = frames.allNodes('n).testResult

      result.signature shouldHaveFields('n -> CTNode)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation)
      result.toSet should equal(Set(n1, n2))
    }
  }
}
