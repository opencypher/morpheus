package org.opencypher.spark.impl.frame

import org.opencypher.spark.api.BinaryRepresentation
import org.opencypher.spark.api.types.CTNode

class ValueAsProductTest extends StdFrameTestSuite {

  import factory._

  test("ValueAsProduct converts DataSet[CypherNode]") {
    val n1 = add(newNode.withLabels("A").withProperties("name" -> "Zippie"))
    val n2 = add(newNode.withLabels("B").withProperties("name" -> "Yggie"))

    new GraphTest {
      import frames._

      val result = ValueAsProduct(allNodes('n)).frameResult

      result.signature shouldHaveFields ('n -> CTNode)
      result.signature shouldHaveFieldSlots ('n -> BinaryRepresentation)
      result.toSet should equal(Set(n1, n2).map(Tuple1(_)))
    }
  }
}
