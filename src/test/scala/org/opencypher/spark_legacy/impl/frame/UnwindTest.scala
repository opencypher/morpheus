/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.spark_legacy.impl.frame

import org.opencypher.spark_legacy.api.frame.BinaryRepresentation
import org.opencypher.spark.api.types.{CTAny, CTNode}
import org.opencypher.spark.api.value.{CypherBoolean, CypherInteger, CypherList}

class UnwindTest extends StdFrameTestSuite {

  test("should unwind a list and increase cardinality") {
    val n = add(newNode.withProperties("list" -> CypherList(Seq(1, 2, 3))))

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'list)('list).unwind('list, 'i).testResult

      result.signature shouldHaveFields('n -> CTNode, 'list -> CTAny.nullable, 'i -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'list -> BinaryRepresentation, 'i -> BinaryRepresentation)
      result.toSet should equal(Set((n, CypherList(Seq(1, 2, 3)), CypherInteger(1)),
                                    (n, CypherList(Seq(1, 2, 3)), CypherInteger(2)),
                                    (n, CypherList(Seq(1, 2, 3)), CypherInteger(3))))
    }
  }

  test("unwinding twice should multiply cardinality") {
    val n = add(newNode.withProperties("list" -> CypherList(Seq(1, 2))))

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'list)('list).unwind('list, 'i).unwind('list, 'j).testResult

      result.signature shouldHaveFields('n -> CTNode, 'list -> CTAny.nullable, 'i -> CTAny.nullable, 'j -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'list -> BinaryRepresentation,
                                            'i -> BinaryRepresentation, 'j -> BinaryRepresentation)
      result.toSet should equal(Set((n, CypherList(Seq(1, 2)), CypherInteger(1), CypherInteger(1)),
                                    (n, CypherList(Seq(1, 2)), CypherInteger(1), CypherInteger(2)),
                                    (n, CypherList(Seq(1, 2)), CypherInteger(2), CypherInteger(1)),
                                    (n, CypherList(Seq(1, 2)), CypherInteger(2), CypherInteger(2))))
    }
  }

  test("unwinding record-dependent lists") {
    val n1 = add(newNode.withProperties("list" -> CypherList(Seq(1, 2))))
    val n2 = add(newNode.withProperties("list" -> CypherList(Seq(null, true))))

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'list)('list).unwind('list, 'i).testResult

      result.signature shouldHaveFields('n -> CTNode, 'list -> CTAny.nullable, 'i -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'list -> BinaryRepresentation, 'i -> BinaryRepresentation)
      result.toSet should equal(Set((n1, CypherList(Seq(1, 2)), CypherInteger(1)),
                                    (n1, CypherList(Seq(1, 2)), CypherInteger(2)),
                                    (n2, CypherList(Seq(null, true)), null),
                                    (n2, CypherList(Seq(null, true)), CypherBoolean(true))))
    }
  }

  test("unwinding an empty list should zero cardinality") {
    add(newNode.withProperties("list" -> CypherList(Seq.empty)))

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'list)('list).unwind('list, 'i).testResult

      result.signature shouldHaveFields('n -> CTNode, 'list -> CTAny.nullable, 'i -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'list -> BinaryRepresentation, 'i -> BinaryRepresentation)
      result.toSet should equal(Set.empty)
    }
  }

  test("unwinding a null list should zero cardinality") {
    add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'list)('list).unwind('list, 'i).testResult

      result.signature shouldHaveFields('n -> CTNode, 'list -> CTAny.nullable, 'i -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'list -> BinaryRepresentation, 'i -> BinaryRepresentation)
      result.toSet should equal(Set.empty)
    }
  }
}
