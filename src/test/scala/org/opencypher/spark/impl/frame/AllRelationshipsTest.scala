package org.opencypher.spark.impl.frame

import org.opencypher.spark.api._
import org.opencypher.spark.api.frame.BinaryRepresentation
import org.opencypher.spark.prototype.api.types._

class AllRelationshipsTest extends StdFrameTestSuite {

  test("AllRelationships produces all input relationships") {
    val n1 = add(newNode.withLabels("A").withProperties("name" -> "Zippie"))
    val n2 = add(newNode.withLabels("B").withProperties("name" -> "Yggie"))
    val n3 = add(newNode.withLabels("C").withProperties("name" -> "Xuxu"))
    val r1 = add(newUntypedRelationship(n1 -> n2))
    val r2 = add(newUntypedRelationship(n1 -> n3))
    val r3 = add(newUntypedRelationship(n2 -> n3))

    new GraphTest {
      val result = frames.allRelationships('r).testResult

      result.signature shouldHaveFields('r -> CTRelationship)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation)
      result.toSet should equal(Set(r1, r2, r3))
    }
  }
}
