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

import org.apache.http.client.utils.URIBuilder
import org.apache.spark.sql.Row
import org.opencypher.caps.impl.spark.io.file.FileCsvGraphSource
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.SparkSessionFixture

class CAPSSessionFileTest extends BaseTestSuite
  with SparkSessionFixture {

  private val testGraphPath = getClass.getResource("/csv/sn").getPath

  private def fileURI: URI = new URI(s"file://$testGraphPath")

  println(fileURI)

  test("File via URI") {
    implicit val capsSession: CAPSSession = CAPSSession.builder(session).build
    val graph = capsSession.graphAt(fileURI)
    graph.nodes("n").toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").toDF().collect.toSet should equal(testGraphRels)
  }

  test("File via mount point") {
    implicit val capsSession: CAPSSession = CAPSSession.builder(session).build
    capsSession.mountSourceAt(FileCsvGraphSource(fileURI), "/test/graph")

    val graph = capsSession.graphAt("/test/graph")
    graph.nodes("n").toDF().collect().toSet should equal(testGraphNodes)
    graph.relationships("rel").toDF().collect.toSet should equal(testGraphRels)
  }

  /**
    * Returns the expected nodes for the test graph in /resources/csv/sn
    *
    * @return expected nodes
    */
  def testGraphNodes: Set[Row] = Set(
    Row(1L, true,  true, false, true,  "Stefan",   42L),
    Row(2L, false, true,  true, true,    "Mats",   23L),
    Row(3L, true,  true, false, true,  "Martin", 1337L),
    Row(4L, true,  true, false, true,     "Max",    8L)
  )

  /**
    * Returns the expected rels for the test graph in /resources/csv/sn
    *
    * @return expected rels
    */
  def testGraphRels: Set[Row] = Set(
    Row(1L, 10L, "KNOWS", 2L, 2016L),
    Row(2L, 20L, "KNOWS", 3L, 2017L),
    Row(3L, 30L, "KNOWS", 4L, 2015L)
  )
}
