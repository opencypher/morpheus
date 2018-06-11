/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl.physical.operators

import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords}
import org.opencypher.spark.testing.CAPSTestSuite

class CAPSPhysicalOperatorTest extends CAPSTestSuite {

  case class TestContent(foo: Int, bar: String)

  val testContents = Seq(TestContent(42, "foo"), TestContent(23, "bar"))
  val testDf = sparkSession.createDataFrame(testContents)

  case class DummyOp(physicalResult: CAPSPhysicalResult) extends LeafPhysicalOperator {

    override val header = physicalResult.records.header

    override def executeLeaf()(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = physicalResult
  }

  test("cache operator with single input") {
    val expectedResult = CAPSPhysicalResult(CAPSRecords.wrap(testDf), CAPSGraph.empty, QualifiedGraphName("foo"))

    val toCache = DummyOp(expectedResult)

    val cache = Cache(toCache)
    cache.execute

    val cacheContent = context.cache(toCache)
    cacheContent should equal(expectedResult)
    cacheContent.records.df.storageLevel should equal(StorageLevel.MEMORY_AND_DISK)
  }

  test("cache operator with cache reuse") {
    val expectedResult = CAPSPhysicalResult(CAPSRecords.wrap(testDf), CAPSGraph.empty, QualifiedGraphName("foo"))

    val toCache0 = DummyOp(expectedResult)
    val toCache1 = DummyOp(expectedResult)

    val r1 = Cache(toCache0).execute
    val r2 = Cache(toCache1).execute
     r1 should equal(r2)
  }
}
