package org.opencypher.spark.impl.frame

import org.opencypher.spark.api._
import org.opencypher.spark.api.frame.BinaryRepresentation
import types.{CTAny, CTNode}
import org.opencypher.spark.api.value.CypherNode

class UnionAllTest extends StdFrameTestSuite {

  test("UnionAll computes union all over its arguments") {
    val zippie = add(newNode.withLabels("A").withProperties("name" -> "Zippie", "age" -> 21, "married" -> true))
    val yggie = add(newNode.withLabels("A", "B").withProperties("name" -> "Yggie", "age" -> 16, "married" -> false))
    val yggdrasil = add(newNode.withLabels("B").withProperties("name" -> "Yggdrasil", "age" ->  10000, "married" -> false))
    val xulu = add(newNode.withLabels("C").withProperties("name" -> "Xulu", "age" -> 5, "married" -> false))

    new GraphTest {
      import frames._

      val lhs =
        allNodes('n)
          .labelFilter("A")
          .asProduct
          .propertyValue('n, 'name)('name)
          .propertyValue('n, 'age)('age)
          .propertyValue('n, 'married)('married)

      val rhs =
        allNodes('n)
          .labelFilter("B")
          .asProduct
          .propertyValue('n, 'name)('name)
          .propertyValue('n, 'age)('age)
          .propertyValue('n, 'married)('married)

      val result = lhs.unionAll(rhs).testResult

      result.signature shouldHaveFields('n -> CTNode, 'name -> CTAny.nullable, 'age -> CTAny.nullable, 'married -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'name -> BinaryRepresentation, 'age -> BinaryRepresentation, 'married -> BinaryRepresentation)

      result.toSet should equal(Set(
          zippie, yggie, yggie, yggdrasil
        ).map { node => (node, CypherNode.property(node)("name"), CypherNode.property(node)("age"), CypherNode.property(node)("married")) }
      )
    }
  }

  test("UnionAll verifies signature compatibility") {
    add(newNode)

    new GraphTest {
      import frames._

      val lhs = allNodes('n1).asProduct
      val rhs = allNodes('n2).asProduct

      val error = the [FrameVerification.FrameSignatureMismatch] thrownBy {
        lhs.unionAll(rhs)
      }
      error.contextName should equal("requireMatchingFrameFields")
    }
  }

  test("UnionAll verifies signature compatibility when fields added asymmetrically") {
    val zippie = add(newNode.withProperties("name" -> "Zippie", "age" -> 21, "married" -> true))
    val yggie = add(newNode.withProperties("name" -> "Yggie", "age" -> 16, "married" -> false))

    new GraphTest {
      import frames._

      val lhs =
        allNodes('n)
          .asProduct
          .propertyValue('n, 'name)('name)
          .propertyValue('n, 'age)('age)
          .propertyValue('n, 'married)('married)

      val rhs =
        allNodes('n)
          .asProduct
          .propertyValue('n, 'married)('married)
          .propertyValue('n, 'name)('name)
          .propertyValue('n, 'age)('age)

      val error = the [FrameVerification.FrameSignatureMismatch] thrownBy {
        lhs.unionAll(rhs)
      }
      error.contextName should equal("requireMatchingFrameFields")
    }
  }
}
