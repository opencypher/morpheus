package org.opencypher.spark.legacy.impl.frame

import org.apache.spark.sql.types.LongType
import org.opencypher.spark.legacy.api.frame.{BinaryRepresentation, EmbeddedRepresentation}
import org.opencypher.spark.prototype.api.types.{CTAny, CTInteger}
import org.opencypher.spark.prototype.api.value.{CypherInteger, CypherString}

class DropFieldTest extends StdFrameTestSuite {

  test("drop a field from two") {
    add(newNode)
    add(newNode)
    add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('a).asProduct.nodeId('a)('aid).dropField('a).testResult

      result.signature shouldHaveFields('aid -> CTInteger)
      result.signature shouldHaveFieldSlots('aid -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(Tuple1(1), Tuple1(2), Tuple1(3)))
    }
  }

  test("drop a field from three") {
    add(newNode.withProperties("prop" -> "foo"))
    add(newNode.withProperties("prop" -> 500))
    add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('a).asProduct.nodeId('a)('aid).propertyValue('a, 'prop)('prop).dropField('a).testResult

      result.signature shouldHaveFields('aid -> CTInteger, 'prop -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('aid -> EmbeddedRepresentation(LongType), 'prop -> BinaryRepresentation)
      result.toSet should equal(Set(1 -> CypherString("foo"), 2 -> CypherInteger(500), 3 -> null))
    }
  }

  test("drop two fields from three") {
    add(newNode.withProperties("prop" -> "foo"))
    add(newNode.withProperties("prop" -> 500))
    add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('a).asProduct.nodeId('a)('aid).propertyValue('a, 'prop)('prop).dropField('a).dropField('aid).testResult

      result.signature shouldHaveFields('prop -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('prop -> BinaryRepresentation)
      result.toSet should equal(Set(Tuple1(CypherString("foo")), Tuple1(CypherInteger(500)), Tuple1(null)))
    }
  }

}
