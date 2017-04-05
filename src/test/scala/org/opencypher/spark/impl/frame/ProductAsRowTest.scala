package org.opencypher.spark.impl.frame

import org.opencypher.spark.api.frame.BinaryRepresentation
import org.opencypher.spark.prototype.api.types.CTNode

class ProductAsRowTest extends StdFrameTestSuite {

  test("ProductAsRow converts products to rows") {
    val a = add(newNode.withLabels("A").withProperties("name" -> "Zippie"))
    val b = add(newNode.withLabels("B").withProperties("name" -> "Yggie"))

    new GraphTest {

      import frames._

      val rowResult = allNodes('n).asProduct.asRow.testResult

      rowResult.signature shouldHaveFields ('n -> CTNode)
      rowResult.signature shouldHaveFieldSlots ('n -> BinaryRepresentation)

      val productResult = allNodes('n).asProduct.asRow.asProduct.testResult

      productResult.toSet should equal(Set(a, b).map(Tuple1(_)))
    }
  }
}
