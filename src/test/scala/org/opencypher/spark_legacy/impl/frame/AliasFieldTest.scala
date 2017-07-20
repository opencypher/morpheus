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
import org.opencypher.spark.api.types.CTNode

class AliasFieldTest extends StdFrameTestSuite {

  test("AliasField renames fields") {
    val a = add(newNode)
    val b = add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.aliasField('n -> 'm).testResult

      result.signature shouldHaveFields('m -> CTNode)
      result.signature shouldHaveFieldSlots('m -> BinaryRepresentation)
      result.toSet should equal(Set(a, b).map(Tuple1(_)))
    }
  }
}
