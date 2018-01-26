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
package org.opencypher.caps.impl.spark

import java.net.URI

import org.apache.http.client.utils.URIBuilder
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.value.{CAPSMap, CAPSNode}
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.impl.spark.io.hdfs.HdfsCsvPropertyGraphDataSource
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.MiniDFSClusterFixture
import org.opencypher.caps.test.support.RecordMatchingTestSupport

import scala.collection.Bag

class CAPSSessionHDFSTest extends CAPSTestSuite with MiniDFSClusterFixture with RecordMatchingTestSupport {

  protected override val dfsTestGraphPath = "/csv/sn"

  protected override def hdfsURI: URI = new URIBuilder(super.hdfsURI).setScheme("hdfs+csv").build()

  test("HDFS via URI") {
    val graph = caps.readFrom(hdfsURI)
    graph.nodes("n").asCaps.toDF().collect().toBag should equal(dfsTestGraphNodes)
    graph.relationships("rel").asCaps.toDF().collect.toBag should equal(dfsTestGraphRels)
  }

  test("HDFS via mount point") {
    caps.mount(
      HdfsCsvPropertyGraphDataSource(hdfsURI, session.sparkContext.hadoopConfiguration, hdfsURI.getPath),
      "/test/graph")

    val graph = caps.readFrom("/test/graph")
    graph.nodes("n").asCaps.toDF().collect().toBag should equal(dfsTestGraphNodes)
    graph.relationships("rel").asCaps.toDF().collect.toBag should equal(dfsTestGraphRels)
  }

  test("HDFS via GRAPH AT") {
    implicit val capsSession = CAPSSession.builder(session).build

    val nodes = capsSession.cypher(s"FROM GRAPH test AT '$hdfsURI' MATCH (n) RETURN n")
    nodes.records.iterator.toBag should equal(
      Bag(
        CAPSMap("n" -> CAPSNode(1L)),
        CAPSMap("n" -> CAPSNode(2L)),
        CAPSMap("n" -> CAPSNode(3L)),
        CAPSMap("n" -> CAPSNode(4L))
      ))

    val edges = capsSession.cypher(s"FROM GRAPH test AT '$hdfsURI' MATCH ()-[r]->() RETURN r")
    edges.records.asCaps.compact.toMaps should equal(
      Bag(
        CAPSMap("r" -> 10L),
        CAPSMap("r" -> 20L),
        CAPSMap("r" -> 30L)
      ))
  }
}
