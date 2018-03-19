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
package org.opencypher.spark.api.io.hdfs

import java.net.URI

import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.{MiniDFSClusterFixture, TeamDataFixture}
import org.opencypher.spark.test.support.RecordMatchingTestSupport

class HdfsCsvPropertyGraphDataSourceTest
  extends CAPSTestSuite with MiniDFSClusterFixture with RecordMatchingTestSupport with TeamDataFixture {

  protected override def dfsTestGraphPath = Some("/csv/sn")

  test("hasGraph should return true for existing graph") {
    val testGraphName = GraphName("sn")

    val dataSource = HdfsCsvPropertyGraphDataSource(
      hadoopConfig = clusterConfig,
      rootPath = "/csv")

    dataSource.hasGraph(testGraphName) should be(true)
  }

  test("hasGraph should return false for non-existing graph") {
    val testGraphName = GraphName("sn2")

    val dataSource = HdfsCsvPropertyGraphDataSource(
      hadoopConfig = clusterConfig,
      rootPath = "/csv")

    dataSource.hasGraph(testGraphName) should be(false)
  }

  test("graphNames should return all names of stored graphs") {
    val testGraphName = GraphName("sn")
    val source = HdfsCsvPropertyGraphDataSource(
      hadoopConfig = clusterConfig,
      rootPath = "/csv")

    source.graphNames should equal(Set(testGraphName))
  }

  test("Load graph from HDFS via DataSource") {
    val testGraphName = GraphName("sn")

    val dataSource = HdfsCsvPropertyGraphDataSource(
      hadoopConfig = clusterConfig,
      rootPath = "/csv")

    val graph = dataSource.graph(testGraphName)
    graph.nodes("n").asCaps.toDF().collect().toBag should equal(csvTestGraphNodes)
    graph.relationships("rel").asCaps.toDF().collect.toBag should equal(csvTestGraphRels)
  }

  test("Load graph from HDFS via Catalog") {
    val testNamespace = Namespace("myHDFS")
    val testGraphName = GraphName("sn")

    val dataSource = HdfsCsvPropertyGraphDataSource(
      hadoopConfig = sparkSession.sparkContext.hadoopConfiguration,
      rootPath = "/csv")

    caps.registerSource(testNamespace, dataSource)

    val nodes = caps.cypher(s"USE GRAPH $testNamespace.$testGraphName MATCH (n) RETURN n")
    nodes.getRecords.asCaps.toDF().collect().toBag should equal(csvTestGraphNodes)

    val edges = caps.cypher(s"USE GRAPH $testNamespace.$testGraphName MATCH ()-[r]->() RETURN r")
    edges.getRecords.asCaps.toDF().collect().toBag should equal(csvTestGraphRelsFromRecords)
  }

  it("is robust about the path input") {
    val paths = Set("hdfs:///foo/bar", "/foo/bar", "hdfs://hostname/foo/bar", "hdfs://hostname:123/foo/bar")

    paths.foreach { path =>
      val uri = HdfsCsvPropertyGraphDataSource(sparkSession.sparkContext.hadoopConfiguration, path).graphPath(GraphName("myGraph"))

      uri should equal(URI.create("hdfs:///foo/bar/myGraph"))
    }
  }
}
