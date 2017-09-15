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
package org.opencypher.caps.impl.spark.io.hdfs

import org.opencypher.caps.api.spark.CAPSGraph
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.{CAPSSessionFixture, MiniDFSClusterFixture, SparkSessionFixture}
import org.scalatest.Matchers

class CsvGraphLoaderAcceptanceTest extends BaseTestSuite
  with SparkSessionFixture
  with CAPSSessionFixture
  with MiniDFSClusterFixture
  with Matchers {

  protected override val dfsTestGraphPath = "/csv"

  test("load csv graph") {
    val loader = new CsvGraphLoader(hdfsURI.toString, session.sparkContext.hadoopConfiguration)

    val graph: CAPSGraph = loader.load
    graph.nodes("n").details.toDF().collect().toSet should equal(dfsTestGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(dfsTestGraphRels)
  }
}
