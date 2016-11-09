package org.opencypher.spark.impl.frame

import org.apache.spark.sql.types.LongType
import org.opencypher.spark.api._
import org.opencypher.spark.api.frame.{BinaryRepresentation, EmbeddedRepresentation}
import types.{CTAny, CTInteger, CTNode, CTRelationship}
import org.opencypher.spark.api.value.{CypherNode, CypherRelationship, CypherString}

class ExtractTest extends StdFrameTestSuite {

  test("Extract.relationshipStartId from RELATIONSHIP") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {
      import frames._

      val result = allRelationships('r).asProduct.relationshipStartId('r)('startId).testResult

      result.signature shouldHaveFields('r -> CTRelationship, 'startId -> CTInteger)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'startId -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(r -> CypherRelationship.startId(r).get.v))
    }
  }

  test("Extract.relationshipStartId from RELATIONSHIP?") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {
      import frames._

      val result = optionalAllRelationships('r).asProduct.relationshipStartId('r)('startId).testResult

      result.signature shouldHaveFields('r -> CTRelationship.nullable, 'startId -> CTInteger.nullable)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'startId -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(r -> CypherRelationship.startId(r).get.v))
    }
  }

  test("Extract.relationshipEndId from RELATIONSHIP") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {
      import frames._

      val result = allRelationships('r).asProduct.relationshipEndId('r)('endId).testResult

      result.signature shouldHaveFields('r -> CTRelationship, 'endId -> CTInteger)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'endId -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(r -> CypherRelationship.endId(r).get.v))
    }
  }

  test("Extract.relationshipEndId from RELATIONSHIP?") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {
      import frames._

      val result = optionalAllRelationships('r).asProduct.relationshipEndId('r)('endId).testResult

      result.signature shouldHaveFields('r -> CTRelationship.nullable, 'endId -> CTInteger.nullable)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'endId -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(r -> CypherRelationship.endId(r).get.v))
    }
  }

  test("Extract.relationshipStartId failing when symbol points to node") {
    new GraphTest {
      import frames._

      val product = allNodes('r).asProduct

      val error = the [FrameVerification.TypeError] thrownBy {
        product.relationshipStartId('r)('startId)
      }
      error.contextName should equal("requireMateriallyIsSubTypeOf")
    }
  }

  test("Extract.relationshipEndId failing when symbol points to node") {
    new GraphTest {
      import frames._

      val product = allNodes('r).asProduct

      val error = the [FrameVerification.TypeError] thrownBy {
        product.relationshipEndId('r)('endId)
      }
      error.contextName should equal("requireMateriallyIsSubTypeOf")
    }
  }

  test("Extract.nodeId from NODE") {
    val a = add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.nodeId('n)('nid).testResult

      result.signature shouldHaveFields('n -> CTNode, 'nid -> CTInteger)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'nid -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(a -> CypherNode.id(a).get.v))
    }
  }

  test("Extract.nodeId from NODE?") {
    val a = add(newNode)

    new GraphTest {

      import frames._

      val result = optionalAllNodes('n).asProduct.nodeId('n)('nid).testResult

      result.signature shouldHaveFields('n -> CTNode.nullable, 'nid -> CTInteger.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'nid -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(a -> CypherNode.id(a).get.v))
    }
  }

  test("Extract.nodeId failing when symbol points to non-node") {
    new GraphTest {

      import frames._

      val product = allRelationships('n).asProduct

      a [FrameVerification.TypeError] shouldBe thrownBy {
        product.nodeId('n)('nid)
      }
    }
  }

  test("Extract.relationshipId from RELATIONSHIP") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {

      import frames._

      val result = allRelationships('r).asProduct.relationshipId('r)('rid).testResult

      result.signature shouldHaveFields('r -> CTRelationship, 'rid -> CTInteger)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'rid -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(r -> CypherNode.id(a).get.v))
    }
  }

  test("Extract.relationshipId from RELATIONSHIP?") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {

      import frames._

      val result = optionalAllRelationships('r).asProduct.relationshipId('r)('rid).testResult

      result.signature shouldHaveFields('r -> CTRelationship.nullable, 'rid -> CTInteger.nullable)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'rid -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(r -> CypherNode.id(a).get.v))
    }
  }

  test("Extract.relationshipId failing when symbol points to non-relationship") {
    new GraphTest {

      import frames._

      val product = allNodes('r).asProduct

      val error = the [FrameVerification.TypeError] thrownBy {
        product.relationshipId('r)('rid)
      }
      error.contextName should equal("requireMateriallyIsSubTypeOf")
    }
  }

  test("Extract.property from NODE") {
    val n1 = add(newNode.withProperties("prop1" -> "foo"))
    val n2 = add(newNode.withProperties("prop2" -> "bar"))
    val n3 = add(newNode.withProperties())

    new GraphTest {
      import frames._

      val frame = allNodes('n).asProduct.propertyValue('n, 'prop1)(Symbol("n.prop1"))
      val result = frame.testResult

      result.signature shouldHaveFields('n -> CTNode, Symbol("n.prop1") -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, Symbol("n.prop1") -> BinaryRepresentation)
      result.toSet should equal(Set(n1 -> CypherString("foo"), n2 -> null, n3 -> null))
    }
  }

  test("Extract.property from NODE?") {
    val n1 = add(newNode.withProperties("prop1" -> "foo"))
    val n2 = add(newNode.withProperties("prop2" -> "bar"))
    val n3 = add(newNode.withProperties())

    new GraphTest {
      import frames._

      val frame = optionalAllNodes('n).asProduct.propertyValue('n, 'prop1)(Symbol("n.prop1"))
      val result = frame.testResult

      result.signature shouldHaveFields('n -> CTNode.nullable, Symbol("n.prop1") -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, Symbol("n.prop1") -> BinaryRepresentation)
      result.toSet should equal(Set(n1 -> CypherString("foo"), n2 -> null, n3 -> null))
    }
  }
}
