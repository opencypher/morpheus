package org.opencypher.spark.legacy.impl.frame

import org.opencypher.spark.legacy.api.frame.BinaryRepresentation
import org.opencypher.spark.prototype.api.types.{CTAny, CTNode}
import org.opencypher.spark.prototype.api.value.CypherValue

class OrderByTest extends StdFrameTestSuite {

  // TODO
  ignore("OrderBy orders by the key in the correct order") {
    val n1 = add(newNode.withProperties("prop" -> 100.5))
    val n2 = add(newNode.withProperties("prop" -> 50))
    val n3 = add(newNode)
    val n4 = add(newNode.withProperties("prop" -> Double.NaN))

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'prop)('v).orderBy(SortItem('v, Asc)).testResult

      result.signature shouldHaveFields('n -> CTNode, 'v -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'v -> BinaryRepresentation)
      result.toList should equal(List[(CypherValue, CypherValue)](n2 -> 50, n1 -> 100.5, n4 -> Double.NaN, n3 -> null))
    }
  }
}
