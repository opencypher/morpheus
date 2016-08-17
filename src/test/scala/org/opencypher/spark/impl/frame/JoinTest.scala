package org.opencypher.spark.impl.frame

import org.apache.spark.sql.types.LongType
import org.opencypher.spark.api.types.{CTInteger, CTNode}
import org.opencypher.spark.api.{BinaryRepresentation, EmbeddedRepresentation}

class JoinTest extends StdFrameTestSuite {

  test("Joins on node ids") {
    add(newLabeledNode("A"))
    val n = add(newLabeledNode("A", "B"))
    val nid = n.id.v
    add(newLabeledNode("B"))

    new GraphTest {
      import frames._

      val lhs = allNodes('l).labelFilter("A").asProduct.nodeId('l)('lid).asRow
      val rhs = allNodes('r).labelFilter("B").asProduct.nodeId('r)('rid).asRow

      val result = lhs.join(rhs).on('lid)('rid).asProduct.testResult

      result.signature shouldHaveFields('l -> CTNode, 'lid -> CTInteger, 'r -> CTNode, 'rid -> CTInteger)
      result.signature shouldHaveFieldSlots(
        'l -> BinaryRepresentation,
        'lid -> EmbeddedRepresentation(LongType),
        'r -> BinaryRepresentation,
        'rid -> EmbeddedRepresentation(LongType)
      )
      result.toSet should equal(Set((n, nid, n, nid)))
    }
  }

  test("Refuses to join using non embedded slots") {
    new GraphTest {
      import frames._

      val lhs = allNodes('l).asRow
      val rhs = allNodes('r).asRow

      val error = the [FrameVerification.SlotNotEmbeddable] thrownBy {
        lhs.join(rhs).on('l)('r)
      }
      error.contextName should equal("requireEmbeddedRepresentation")
    }
  }

  test("Refuses to join on incompatible cypher types") {
    new GraphTest {
      import frames._

      val lhs = allNodes('l).asProduct.nodeId('l)('id).asRow
      val rhs = allRelationships('r).asProduct.relationshipType('r)('typ).asRow

      val error = the [FrameVerification.UnInhabitedMeetType] thrownBy {
        lhs.join(rhs).on('id)('typ)
      }
      error.contextName should equal("requireInhabitedMeetType")
    }
  }
}
