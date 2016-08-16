package org.opencypher.spark.impl.frame

import org.apache.spark.sql.types.IntegerType
import org.opencypher.spark.api._
import org.opencypher.spark.api.types.{CTAny, CTInteger, CTNode, CTRelationship}
import org.opencypher.spark.impl.{FrameVerification, FrameVerificationError, ProductFrame}

class ProjectFromEntityTest extends StdFrameTestSuite {

  test("ProjectFromEntity.relationshipStartId") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {

      import frames._

      val result = allRelationships('r).asProduct.relationshipStartId('r)('startId).frameResult

      result.signature shouldHaveFields('r -> CTRelationship, 'startId -> CTInteger)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'startId -> EmbeddedRepresentation(IntegerType))
      result.toSet should equal(Set(r -> r.startId.v))
    }
  }

  test("ProjectFromEntity.relationshipEndId") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {

      import frames._

      val result = allRelationships('r).asProduct.relationshipEndId('r)('endId).frameResult

      result.signature shouldHaveFields('r -> CTRelationship, 'endId -> CTInteger)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'endId -> EmbeddedRepresentation(IntegerType))
      result.toSet should equal(Set(r -> r.endId.v))
    }
  }

  test("ProjectFromEntity.relationshipStartId failing when symbol points to node") {
    new GraphTest {

      import frames._

        val product = allNodes('r).asProduct

      a [ProjectFromEntity.CypherTypeError] shouldBe thrownBy {
        product.relationshipStartId('r)('startId)
      }
    }
  }

  test("ProjectFromEntity.relationshipEndId failing when symbol points to node") {
    new GraphTest {

      import frames._

        val product = allNodes('r).asProduct

      a [ProjectFromEntity.CypherTypeError] shouldBe thrownBy {
        product.relationshipEndId('r)('endId)
      }
    }
  }
}
