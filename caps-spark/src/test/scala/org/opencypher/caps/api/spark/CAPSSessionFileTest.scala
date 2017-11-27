/*
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

import org.apache.spark.sql.Row
import org.opencypher.caps.impl.spark.io.file.FileCsvGraphSource
import org.opencypher.caps.test.CAPSTestSuite

import scala.collection.Bag

class CAPSSessionFileTest extends CAPSTestSuite {

  private val testGraphPath = getClass.getResource("/csv/sn").getPath

  private def fileURI: URI = new URI(s"file+csv://$testGraphPath")

  test("File via URI") {
    val graph = caps.graphAt(fileURI)
    graph.nodes("n").toDF().collect().toBag should equal(testGraphNodes)
    graph.relationships("rel").toDF().collect.toBag should equal(testGraphRels)
  }

  test("File via mount point") {
    caps.mountSourceAt(FileCsvGraphSource(fileURI), "/test/graph")
    val graph = caps.graphAt("/test/graph")
    graph.nodes("n").toDF().collect().toBag should equal(testGraphNodes)
    graph.relationships("rel").toDF().collect.toBag should equal(testGraphRels)
  }

  /**
    * Returns the expected nodes for the test graph in /resources/csv/sn
    *
    * @return expected nodes
    */
  def testGraphNodes: Bag[Row] = Bag(
    Row(1L, true, true, true, false, 42L, "Stefan"),
    Row(2L, true, false, true, true, 23L, "Mats"),
    Row(3L, true, true, true, false, 1337L, "Martin"),
    Row(4L, true, true, true, false, 8L, "Max")
  )

  /**
    * Returns the expected rels for the test graph in /resources/csv/sn
    *
    * @return expected rels
    */
  def testGraphRels: Bag[Row] = Bag(
    Row(1L, 10L, "KNOWS", 2L, 2016L),
    Row(2L, 20L, "KNOWS", 3L, 2017L),
    Row(3L, 30L, "KNOWS", 4L, 2015L)
  )
}
