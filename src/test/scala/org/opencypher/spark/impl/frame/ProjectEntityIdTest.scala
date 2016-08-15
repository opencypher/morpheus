package org.opencypher.spark.impl.frame

import org.apache.spark.sql.types.IntegerType
import org.opencypher.spark.api.{BinaryRepresentation, EmbeddedRepresentation}
import org.opencypher.spark.api.types.{CTInteger, CTNode, CTRelationship}

class ProjectEntityIdTest extends StdFrameTestSuite {

  test("ProjectEntityId should project node id") {
    val n1 = add(newNode)
    val n2 = add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.projectId('n)('nid).frameResult

      result.signature shouldHaveFields('n -> CTNode, 'nid -> CTInteger)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'nid -> EmbeddedRepresentation(IntegerType))
      result.toSet should equal(Set(n1 -> 1, n2 -> 2))
    }
  }

  test("ProjectEntityId should project relationship id") {
    val n1 = add(newNode)
    val n2 = add(newNode)
    val r1 = add(newUntypedRelationship(n1 -> n2))
    val r2 = add(newUntypedRelationship(n2 -> n2))

    new GraphTest {
      import frames._

      val result = allRelationships('r).asProduct.projectId('r)('rid).frameResult

      result.signature shouldHaveFields('r -> CTRelationship, 'rid -> CTInteger)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'rid -> EmbeddedRepresentation(IntegerType))
      result.toSet should equal(Set(r1 -> 1, r2 -> 2))
    }

  }

}
