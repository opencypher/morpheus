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
package org.opencypher.caps.api.ir

import org.opencypher.caps.api.ir.block._
import org.opencypher.caps.impl.ir.IrTestSuite

class QueryModelTest extends IrTestSuite {

  val block_a = BlockRef("a")
  val block_b = BlockRef("b")
  val block_c = BlockRef("c")
  val block_d = BlockRef("d")
  val block_e = BlockRef("e")

  test("dependencies") {
    val model = irFor(block_a, Map(
      block_a -> DummyBlock(Set(block_b, block_c)),
      block_b -> DummyBlock(),
      block_c -> DummyBlock()
    )).model

    model.dependencies(block_a) should equal(Set(block_b, block_c))
    model.dependencies(block_b) should equal(Set.empty)
    model.dependencies(block_c) should equal(Set.empty)
  }

  test("all_dependencies") {
    val model = irFor(block_a, Map(
      block_a -> DummyBlock(Set(block_b, block_c)),
      block_b -> DummyBlock(Set(block_d)),
      block_c -> DummyBlock(),
      block_d -> DummyBlock(Set(block_e)),
      block_e -> DummyBlock()
    )).model

    model.allDependencies(block_a) should equal(Set(block_b, block_c, block_d, block_e))
    model.allDependencies(block_b) should equal(Set(block_d, block_e))
    model.allDependencies(block_c) should equal(Set.empty)
    model.allDependencies(block_d) should equal(Set(block_e))
    model.allDependencies(block_e) should equal(Set.empty)
  }

  test("handle loops") {
    val model = irFor(block_a, Map(
      block_a -> DummyBlock(Set(block_b, block_c)),
      block_b -> DummyBlock(Set(block_d)),
      block_c -> DummyBlock(Set(block_b)),
      block_d -> DummyBlock(Set(block_c))
    )).model

    an [IllegalStateException] shouldBe thrownBy {
      model.allDependencies(block_a)
    }
    an [IllegalStateException] shouldBe thrownBy {
      model.allDependencies(block_b)
    }
    an [IllegalStateException] shouldBe thrownBy {
      model.allDependencies(block_c)
    }
    an [IllegalStateException] shouldBe thrownBy {
      model.allDependencies(block_d)
    }
  }
}
