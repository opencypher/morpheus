package org.opencypher.spark.impl.frame

import org.opencypher.spark.CypherTypes.CTNode
import org.opencypher.spark._

class AllNodesTest extends StdFrameTestSuite {

  import CypherValue.implicits._
  import factory._
  import org.opencypher.spark.EntityData._

  test("AllNodes frame produces all input nodes") {
    val n1 = add(newNode.withLabels("A").withProperties("name" -> "Zippie"))
    val n2 = add(newNode.withLabels("B").withProperties("name" -> "Yggie"))
    val nodes = session.createDataset(List(n1, n2))

    val result = AllNodes(nodes)('n).frameResult

    result.signature shouldHaveFields 'n -> CTNode
    result.signature shouldHaveFieldSlots 'n -> BinaryRepresentation
    result.toSet should equal(Set(n1, n2))
  }
}
