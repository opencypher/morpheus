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
import org.opencypher.spark.api.types.CTAny
import org.opencypher.spark.api.value.{CypherBoolean, CypherInteger}

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
        CypherInteger(21) -> CypherBoolean.TRUE,
        CypherInteger(16) -> CypherBoolean.FALSE
      ))
    }
  }
}
