package org.opencypher.spark.impl.frame

import org.opencypher.spark.api.BinaryRepresentation
import org.opencypher.spark.api.types.CTNode
import org.scalatest.FunSuite

class LabelFilterNodeTest extends StdFrameTestSuite {

  import factory._

  test("LabelFilter filters out nodes without correct label") {
    val a = add(newLabeledNode("A"))
    add(newLabeledNode("a"))
    val b = add(newLabeledNode("A", "B"))
    add(newLabeledNode("NotA"))
    add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).labelFilter("A").frameResult

      result.signature shouldHaveFields('n -> CTNode)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation)
      result.toSet should equal(Set(a, b))
    }
  }

  test("LabelFilter filters out nodes without correct labels") {
    val a = add(newLabeledNode("A"))
    add(newLabeledNode("a"))
    val b = add(newLabeledNode("A", "B"))
    add(newLabeledNode("NotA"))
    add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).labelFilter("A", "B").frameResult

      result.signature shouldHaveFields('n -> CTNode)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation)
      result.toSet should equal(Set(b))
    }
  }

}
