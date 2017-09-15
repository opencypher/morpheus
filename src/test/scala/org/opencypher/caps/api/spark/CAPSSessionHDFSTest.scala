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
package org.opencypher.caps.api.spark

import org.opencypher.caps.impl.spark.io.hdfs.HdfsCsvGraphSource
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.{MiniDFSClusterFixture, SparkSessionFixture}

class CAPSSessionHDFSTest extends BaseTestSuite
  with SparkSessionFixture
  with MiniDFSClusterFixture {

  protected override val dfsTestGraphPath = "/csv/sn"

  test("HDFS via URI") {
    implicit val capsSession: CAPSSession = CAPSSession.builder(session).build
    val graph = capsSession.graphAt(hdfsURI)
    graph.nodes("n").details.toDF().collect().toSet should equal(dfsTestGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(dfsTestGraphRels)
  }

  test("HDFS via mount point") {
    implicit val capsSession: CAPSSession = CAPSSession.builder(session).build
    capsSession.mountSourceAt(HdfsCsvGraphSource(hdfsURI, session.sparkContext.hadoopConfiguration, hdfsURI.getPath), "/test/graph")

    val graph = capsSession.graphAt("/test/graph")
    graph.nodes("n").details.toDF().collect().toSet should equal(dfsTestGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(dfsTestGraphRels)
  }
}
