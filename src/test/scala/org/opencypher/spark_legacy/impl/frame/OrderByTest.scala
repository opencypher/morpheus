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
import org.opencypher.spark.api.value.CypherValue

class OrderByTest extends StdFrameTestSuite {

  // TODO
  ignore("OrderBy orders by the key in the correct order") {
    val n1 = add(newNode.withProperties("prop" -> 100.5))
    val n2 = add(newNode.withProperties("prop" -> 50))
    val n3 = add(newNode)
    val n4 = add(newNode.withProperties("prop" -> Double.NaN))

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'prop)('v).orderBy(SortItem('v, Asc)).testResult

      result.signature shouldHaveFields('n -> CTNode, 'v -> CTAny.nullable)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'v -> BinaryRepresentation)
      result.toList should equal(List[(CypherValue, CypherValue)](n2 -> 50, n1 -> 100.5, n4 -> Double.NaN, n3 -> null))
    }
  }
}
