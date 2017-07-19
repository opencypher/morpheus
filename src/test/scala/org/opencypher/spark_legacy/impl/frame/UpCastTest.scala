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
import org.opencypher.spark.api.types.CTMap

class UpCastTest extends StdFrameTestSuite {

  test("Can upcast from NODE to MAP") {
    val n = add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).upcast('n)(_ => CTMap).testResult

      result.signature shouldHaveFields('n -> CTMap)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation)
      result.toSet should equal(Set(n))
    }
  }
}
