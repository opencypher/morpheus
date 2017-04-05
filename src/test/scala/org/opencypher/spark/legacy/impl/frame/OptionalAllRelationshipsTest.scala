package org.opencypher.spark.legacy.impl.frame

import org.opencypher.spark.legacy.api.frame.BinaryRepresentation
import org.opencypher.spark.prototype.api.types.{CTRelationship, CTRelationshipOrNull}

class OptionalAllRelationshipsTest extends StdFrameTestSuite {

  test("should get null on empty") {
    // empty graph

    new GraphTest {
      import frames._

      val result = optionalAllRelationships('a).testResult

      result.signature shouldHaveFields('a -> CTRelationship.nullable)
      result.signature shouldHaveFieldSlots('a -> BinaryRepresentation)
      result.toSet should equal(Set(null))
    }
  }

  test("should get null on empty 2") {
    add(newNode)
    add(newNode)

    new GraphTest {
      import frames._

      val result = optionalAllRelationships('a).testResult

      result.signature shouldHaveFields('a -> CTRelationshipOrNull)
      result.signature shouldHaveFieldSlots('a -> BinaryRepresentation)
      result.toSet should equal(Set(null))
    }
  }

  test("should get relationships if present") {
    val a = add(newUntypedRelationship(add(newNode), add(newNode)))

    new GraphTest {
      import frames._

      val result = optionalAllRelationships('a).testResult

      result.signature shouldHaveFields('a -> CTRelationshipOrNull)
      result.signature shouldHaveFieldSlots('a -> BinaryRepresentation)
      result.toSet should equal(Set(a))
    }
  }

}
