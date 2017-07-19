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

import org.opencypher.spark_legacy.api._
import org.opencypher.spark_legacy.api.frame.BinaryRepresentation
import org.opencypher.spark.api.types._

class AllRelationshipsTest extends StdFrameTestSuite {

  test("AllRelationships produces all input relationships") {
    val n1 = add(newNode.withLabels("A").withProperties("name" -> "Zippie"))
    val n2 = add(newNode.withLabels("B").withProperties("name" -> "Yggie"))
    val n3 = add(newNode.withLabels("C").withProperties("name" -> "Xuxu"))
    val r1 = add(newUntypedRelationship(n1 -> n2))
    val r2 = add(newUntypedRelationship(n1 -> n3))
    val r3 = add(newUntypedRelationship(n2 -> n3))

    new GraphTest {
      val result = frames.allRelationships('r).testResult

      result.signature shouldHaveFields('r -> CTRelationship)
      result.signature shouldHaveFieldSlots('r -> BinaryRepresentation)
      result.toSet should equal(Set(r1, r2, r3))
    }
  }
}
