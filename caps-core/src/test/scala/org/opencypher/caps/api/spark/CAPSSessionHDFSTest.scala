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

import org.apache.http.client.utils.URIBuilder
import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.demo.Configuration.PrintLogicalPlan
import org.opencypher.caps.impl.spark.io.hdfs.HdfsCsvGraphSource
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.{MiniDFSClusterFixture, SparkSessionFixture}
import org.opencypher.caps.test.support.RecordMatchingTestSupport

import scala.collection.Bag

class CAPSSessionHDFSTest
    extends BaseTestSuite
    with SparkSessionFixture
    with MiniDFSClusterFixture
    with RecordMatchingTestSupport {

  protected override val dfsTestGraphPath = "/csv/sn"

  protected override def hdfsURI: URI = new URIBuilder(super.hdfsURI).setScheme("hdfs+csv").build()

  test("HDFS via URI") {
    implicit val capsSession: CAPSSession = CAPSSession.builder(session).build
    val graph                             = capsSession.graphAt(hdfsURI)
    graph.nodes("n").toDF().collect().toSet should equal(dfsTestGraphNodes)
    graph.relationships("rel").toDF().collect.toSet should equal(dfsTestGraphRels)
  }

  test("HDFS via mount point") {
    implicit val capsSession: CAPSSession = CAPSSession.builder(session).build
    capsSession.mountSourceAt(
      HdfsCsvGraphSource(hdfsURI, session.sparkContext.hadoopConfiguration, hdfsURI.getPath),
      "/test/graph")

    val graph = capsSession.graphAt("/test/graph")
    graph.nodes("n").toDF().collect().toSet should equal(dfsTestGraphNodes)
    graph.relationships("rel").toDF().collect.toSet should equal(dfsTestGraphRels)
  }

  test("HDFS via GRAPH AT") {
    implicit val capsSession: CAPSSession = CAPSSession.builder(session).build

    val nodes = capsSession.cypher(s"FROM GRAPH test AT '$hdfsURI' MATCH (n) RETURN n")
    nodes.records.compact.toMaps should equal(
      Bag(
        CypherMap("n" -> 1L),
        CypherMap("n" -> 2L),
        CypherMap("n" -> 3L),
        CypherMap("n" -> 4L)
      ))

    val edges = capsSession.cypher(s"FROM GRAPH test AT '$hdfsURI' MATCH ()-[r]->() RETURN r")
    edges.records.compact.toMaps should equal(
      Bag(
        CypherMap("r" -> 10L),
        CypherMap("r" -> 20L),
        CypherMap("r" -> 30L)
      ))
  }
}
