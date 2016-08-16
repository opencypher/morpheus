package org.opencypher.spark.impl.frame

import org.opencypher.spark.api._
import org.opencypher.spark.api.types.CTAny

class SelectProductFieldsTest extends StdFrameTestSuite {

  test("SelectProductFields selects the correct fields") {
    add(newNode.withLabels("A").withProperties("name" -> "Zippie", "age" -> 21, "married" -> true))
    add(newNode.withLabels("B").withProperties("name" -> "Yggie", "age" -> 16, "married" -> false))

    new GraphTest {

      import frames._

      val result =
        allNodes('n)
          .asProduct
          .propertyValue('n, 'name)('name)
          .propertyValue('n, 'age)('age)
          .propertyValue('n, 'married)('married)
          .selectFields('age, 'married)
          .testResult

      result.signature shouldHaveFields ('age -> CTAny.nullable, 'married -> CTAny.nullable)
      result.signature shouldHaveFieldSlots ('age -> BinaryRepresentation, 'married -> BinaryRepresentation)

      result.toSet should equal(Set(
        CypherInteger(21) -> CypherTrue,
        CypherInteger(16) -> CypherFalse
      ))
    }
  }
}
