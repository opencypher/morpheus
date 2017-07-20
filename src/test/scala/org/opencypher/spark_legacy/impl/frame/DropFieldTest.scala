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

import org.apache.spark.sql.types.LongType
import org.opencypher.spark_legacy.api.frame.{BinaryRepresentation, EmbeddedRepresentation}
import org.opencypher.spark.api.types.{CTAny, CTInteger}
import org.opencypher.spark.api.value.{CypherInteger, CypherString}

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
