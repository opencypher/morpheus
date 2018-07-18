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

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.impl.operators.{Cache, RelationalOperator}
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable
import org.opencypher.spark.testing.CAPSTestSuite

//TODO: Rewrite or delete when refactoring is complete
//class CAPSPhysicalOperatorTest extends CAPSTestSuite {
//
//  case class TestContent(foo: Int, bar: String)
//
//  val testContents: Seq[TestContent] = Seq(TestContent(42, "foo"), TestContent(23, "bar"))
//  val testDf: DataFrame = sparkSession.createDataFrame(testContents)
//
//  case class DummyOp(physicalResult: CAPSResult)(implicit override val context: RelationalRuntimeContext[DataFrameTable]) extends RelationalOperator[DataFrameTable] {
//
//    override lazy val header: RecordHeader = physicalResult.records.header
//
//    override lazy val _table: DataFrameTable = physicalResult.records.table
//
//  }
//
//  test("cache operator with single input") {
//    val expectedResult = CAPSResult(caps.records.wrap(testDf), RelationalCypherGraph[DataFrameTable].empty, QualifiedGraphName("foo"))
//
//    val toCache = DummyOp(expectedResult)
//
//    val cache = Cache(toCache)
//    cache.table.size // Force execution
//
//    val cachedDf = context.cache(toCache).df
//    cachedDf should equal(expectedResult.records.df)
//    cachedDf.storageLevel should equal(StorageLevel.MEMORY_AND_DISK)
//  }
//
//  test("cache operator with cache reuse") {
//    val expectedResult = CAPSPhysicalResult(caps.records.wrap(testDf), RelationalCypherGraph[DataFrameTable].empty, QualifiedGraphName("foo"))
//
//    val toCache0 = DummyOp(expectedResult)
//    val toCache1 = DummyOp(expectedResult)
//
//    val r1 = Cache(toCache0).table
//    val r2 = Cache(toCache1).table
//     r1 should equal(r2)
//  }
//}
