package org.opencypher.spark.impl.frame

import org.opencypher.spark.api.BinaryRepresentation
import org.opencypher.spark.api.types.{CTAny, CTNode}

class OrderByTest extends StdFrameTestSuite {

  ignore("OrderBy orders by the key in the correct order") {
    // TODO: Doesn't work due to mixed CypherValue types...

    val n1 = add(newNode.withProperties("prop" -> 100.5))
    val n2 = add(newNode.withProperties("prop" -> 50))
    val n3 = add(newNode)
    val n4 = add(newNode.withProperties("prop" -> Double.NaN))

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'prop)('v).orderBy('v).testResult

      result.signature shouldHaveFields('n -> CTNode, 'v -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'v -> BinaryRepresentation)
      result.toList should equal(List(n2, n1, n4, n3))
    }
  }
}
