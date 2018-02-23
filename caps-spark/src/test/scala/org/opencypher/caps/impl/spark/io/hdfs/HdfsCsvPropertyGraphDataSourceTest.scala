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
package org.opencypher.caps.impl.spark.io.hdfs

import org.opencypher.caps.api.graph.{GraphName, Namespace}
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.{MiniDFSClusterFixture, TeamDataFixture}
import org.opencypher.caps.test.support.RecordMatchingTestSupport

class HdfsCsvPropertyGraphDataSourceTest
  extends CAPSTestSuite with MiniDFSClusterFixture with RecordMatchingTestSupport with TeamDataFixture {

  protected override def dfsTestGraphPath = "/csv/sn"

  test("hasGraph should return true for existing graph") {
    val testGraphName = GraphName.from("sn")

    val dataSource = new HdfsCsvPropertyGraphDataSource(
      hadoopConfig = clusterConfig,
      rootPath = "/csv")

    dataSource.hasGraph(testGraphName) should be(true)
  }

  test("hasGraph should return false for non-existing graph") {
    val testGraphName = GraphName.from("sn2")

    val dataSource = new HdfsCsvPropertyGraphDataSource(
      hadoopConfig = clusterConfig,
      rootPath = "/csv")

    dataSource.hasGraph(testGraphName) should be(false)
  }

  test("graphNames should return all names of stored graphs") {
    val testGraphName = GraphName.from("sn")
    val source = new HdfsCsvPropertyGraphDataSource(
      hadoopConfig = clusterConfig,
      rootPath = "/csv")

    source.graphNames should equal(Set(testGraphName))
  }

  test("Load graph from HDFS via DataSource") {
    val testGraphName = GraphName.from("sn")

    val dataSource = new HdfsCsvPropertyGraphDataSource(
      hadoopConfig = clusterConfig,
      rootPath = "/csv")

    val graph = dataSource.graph(testGraphName)
    graph.nodes("n").asCaps.toDF().collect().toBag should equal(csvTestGraphNodes)
    graph.relationships("rel").asCaps.toDF().collect.toBag should equal(csvTestGraphRels)
  }

  test("Load graph from HDFS via Catalog") {
    val testNamespace = Namespace.from("myHDFS")
    val testGraphName = GraphName.from("sn")

    val dataSource = new HdfsCsvPropertyGraphDataSource(
      hadoopConfig = sparkSession.sparkContext.hadoopConfiguration,
      rootPath = "/csv")

    caps.registerSource(testNamespace, dataSource)

    val nodes = caps.cypher(s"FROM GRAPH AT '$testNamespace.$testGraphName' MATCH (n) RETURN n")
    nodes.records.asCaps.toDF().collect().toBag should equal(csvTestGraphNodes)

    val edges = caps.cypher(s"FROM GRAPH AT '$testNamespace.$testGraphName' MATCH ()-[r]->() RETURN r")
    edges.records.asCaps.toDF().collect().toBag should equal(csvTestGraphRelsFromRecords)
  }
}
