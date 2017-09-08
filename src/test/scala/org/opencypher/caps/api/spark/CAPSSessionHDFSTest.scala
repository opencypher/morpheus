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

import java.net.URI

import org.opencypher.caps.api.io.hdfs.HdfsCsvGraphSource
import org.opencypher.caps.{HDFSTestSession, SparkTestSession}
import org.scalatest.{FunSuite, Matchers}

class CAPSSessionHDFSTest extends FunSuite
  with SparkTestSession.Fixture
  with HDFSTestSession.Fixture
  with Matchers {

  test("HDFS via URI") {
    val graph = CAPSSession.builder(session).build.withGraphAt(hdfsURI)
    graph.nodes("n").details.toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(testGraphRels)
  }

  test("HDFS via mount point") {
    val capsSession = CAPSSession.builder(session)
      .withGraphSource("/test/graph", HdfsCsvGraphSource(hdfsURI, session.sparkContext.hadoopConfiguration, hdfsURI.getPath))
      .build

    val graph = capsSession.withGraphAt(URI.create("/test/graph"))
    graph.nodes("n").details.toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").details.toDF().collect.toSet should equal(testGraphRels)
  }
}
