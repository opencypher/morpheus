/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.spark.physical.operators

import org.apache.spark.storage.StorageLevel
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.impl.spark.physical.{PhysicalResult, RuntimeContext}
import org.opencypher.caps.test.CAPSTestSuite

class PhysicalOperatorTest extends CAPSTestSuite {

  case class TestContent(foo: Int, bar: String)

  val testContents = Seq(TestContent(42, "foo"), TestContent(23, "bar"))

  case class DummyOp(physicalResult: PhysicalResult) extends LeafPhysicalOperator {

    override def executeLeaf()(implicit context: RuntimeContext): PhysicalResult = physicalResult
  }

  test("cache operator with single input") {
    val expectedResult = PhysicalResult(CAPSRecords.create(testContents), Map.empty)

    val toCache = DummyOp(expectedResult)

    val cache = Cache(toCache)
    cache.execute

    val cacheContent = context.cache(toCache)
    cacheContent should equal(expectedResult)
    cacheContent.records.data.storageLevel should equal(StorageLevel.MEMORY_AND_DISK)
  }

  test("cache operator with cache reuse") {
    val expectedResult = PhysicalResult(CAPSRecords.create(testContents), Map.empty)

    val toCache0 = DummyOp(expectedResult)
    val toCache1 = DummyOp(expectedResult)

    val r1 = Cache(toCache0).execute
    val r2 = Cache(toCache1).execute
     r1 should equal(r2)
  }
}
