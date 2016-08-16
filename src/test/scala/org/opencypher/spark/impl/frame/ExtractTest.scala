package org.opencypher.spark.impl.frame

import org.apache.spark.sql.types.IntegerType
import org.opencypher.spark.api._
import org.opencypher.spark.api.types.{CTAny, CTInteger, CTNode, CTRelationship}

class ExtractTest extends StdFrameTestSuite {

  test("Extract.relationshipStartId from RELATIONSHIP") {
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

  test("Extract.relationshipStartId from RELATIONSHIP?") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {

      import frames._

      val result = allRelationships('r).asProduct.nullable('r).relationshipStartId('r)('startId).frameResult

      result.signature shouldHaveFields('r -> CTRelationship.nullable, 'startId -> CTInteger.nullable)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'startId -> EmbeddedRepresentation(IntegerType))
      result.toSet should equal(Set(r -> r.startId.v))
    }
  }

  test("Extract.relationshipEndId from RELATIONSHIP") {
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

  test("Extract.relationshipEndId from RELATIONSHIP?") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {

      import frames._

      val result = allRelationships('r).asProduct.nullable('r).relationshipEndId('r)('endId).frameResult

      result.signature shouldHaveFields('r -> CTRelationship.nullable, 'endId -> CTInteger.nullable)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'endId -> EmbeddedRepresentation(IntegerType))
      result.toSet should equal(Set(r -> r.endId.v))
    }
  }

  test("Extract.relationshipStartId failing when symbol points to node") {
    new GraphTest {

      import frames._

        val product = allNodes('r).asProduct

      a [Extract.CypherTypeError] shouldBe thrownBy {
        product.relationshipStartId('r)('startId)
      }
    }
  }

  test("Extract.relationshipEndId failing when symbol points to node") {
    new GraphTest {

      import frames._

        val product = allNodes('r).asProduct

      a [Extract.CypherTypeError] shouldBe thrownBy {
        product.relationshipEndId('r)('endId)
      }
    }
  }

  test("Extract.nodeId from NODE") {
    val a = add(newNode)

    new GraphTest {

      import frames._

      val result = allNodes('n).asProduct.nodeId('n)('nid).frameResult

      result.signature shouldHaveFields('n -> CTNode, 'nid -> CTInteger)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'nid -> EmbeddedRepresentation(IntegerType))
      result.toSet should equal(Set(a -> a.id.v))
    }
  }

  test("Extract.nodeId from NODE?") {
    val a = add(newNode)

    new GraphTest {

      import frames._

      val result = allNodes('n).asProduct.nullable('n).nodeId('n)('nid).frameResult

      result.signature shouldHaveFields('n -> CTNode.nullable, 'nid -> CTInteger.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'nid -> EmbeddedRepresentation(IntegerType))
      result.toSet should equal(Set(a -> a.id.v))
    }
  }

  test("Extract.nodeId failing when symbol points to non-node") {
    new GraphTest {

      import frames._

      val product = allRelationships('n).asProduct

      a [Extract.CypherTypeError] shouldBe thrownBy {
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

      val result = allRelationships('r).asProduct.relationshipId('r)('rid).frameResult

      result.signature shouldHaveFields('r -> CTRelationship, 'rid -> CTInteger)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'rid -> EmbeddedRepresentation(IntegerType))
      result.toSet should equal(Set(r -> r.id.v))
    }
  }

  test("Extract.relationshipId from RELATIONSHIP?") {
    val a = add(newNode)
    val b = add(newNode)
    val r = add(newUntypedRelationship(a -> b))

    new GraphTest {

      import frames._

      val result = allRelationships('r).asProduct.nullable('r).relationshipId('r)('rid).frameResult

      result.signature shouldHaveFields('r -> CTRelationship.nullable, 'rid -> CTInteger.nullable)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation, 'rid -> EmbeddedRepresentation(IntegerType))
      result.toSet should equal(Set(r -> r.id.v))
    }
  }

  test("Extract.relationshipId failing when symbol points to non-relationship") {
    new GraphTest {

      import frames._

      val product = allNodes('r).asProduct

      a [Extract.CypherTypeError] shouldBe thrownBy {
        product.relationshipId('r)('rid)
      }
    }
  }

  test("Extract.property from NODE") {
    val n1 = add(newNode.withProperties("prop1" -> "foo"))
    val n2 = add(newNode.withProperties("prop2" -> "bar"))
    val n3 = add(newNode.withProperties())

    new GraphTest {
      import frames._

      val frame = allNodes('n).asProduct.property('n, 'prop1)(Symbol("n.prop1"))
      val result = frame.frameResult

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

      val frame = allNodes('n).asProduct.nullable('n).property('n, 'prop1)(Symbol("n.prop1"))
      val result = frame.frameResult

      result.signature shouldHaveFields('n -> CTNode.nullable, Symbol("n.prop1") -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, Symbol("n.prop1") -> BinaryRepresentation)
      result.toSet should equal(Set(n1 -> CypherString("foo"), n2 -> null, n3 -> null))
    }
  }
}
