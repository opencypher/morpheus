package org.opencypher.spark.impl.frame

import org.opencypher.spark.api.types.{CTAny, CTNode}
import org.opencypher.spark.api.{BinaryRepresentation, CypherString}

class GetNodePropertyTest extends StdFrameTestSuite {

  import factory._

  test("GetNodeProperty gets a property from a node") {
    val n1 = add(newNode.withProperties("prop1" -> "foo"))
    val n2 = add(newNode.withProperties("prop2" -> "bar"))
    val n3 = add(newNode.withProperties())


    new GraphTest {
      import frames._

      val frame = allNodes('n).valuesAsProduct.getNodeProperty('n, 'prop1)(Symbol("n.prop1"))
      val result = frame.frameResult

      result.signature shouldHaveFields('n -> CTNode, Symbol("n.prop1") -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, Symbol("n.prop1") -> BinaryRepresentation)
      result.toSet should equal(Set(n1 -> CypherString("foo"), n2 -> null, n3 -> null))
    }
  }
}
