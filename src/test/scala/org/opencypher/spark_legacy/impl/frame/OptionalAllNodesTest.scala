package org.opencypher.spark_legacy.impl.frame

import org.opencypher.spark_legacy.api.frame.BinaryRepresentation
import org.opencypher.spark.api.types.CTNode

class OptionalAllNodesTest extends StdFrameTestSuite {

  test("should get null on empty") {
    // empty graph

    new GraphTest {
      import frames._

      val result = optionalAllNodes('a).testResult

      result.signature shouldHaveFields('a -> CTNode.nullable)
      result.signature shouldHaveFieldSlots('a -> BinaryRepresentation)
      result.toSet should equal(Set(null))
    }
  }

  test("should get nodes if present") {
    val a = add(newNode)

    new GraphTest {
      import frames._

      val result = optionalAllNodes('a).testResult

      result.signature shouldHaveFields('a -> CTNode.nullable)
      result.signature shouldHaveFieldSlots('a -> BinaryRepresentation)
      result.toSet should equal(Set(a))
    }
  }

}
