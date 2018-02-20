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

import org.opencypher.caps.api.io.{GraphName, Namespace}
import org.opencypher.caps.api.value.CAPSNode
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.impl.spark.io.hdfs.HdfsCsvPropertyGraphDataSource
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.MiniDFSClusterFixture
import org.opencypher.caps.test.support.RecordMatchingTestSupport

import scala.collection.Bag

class CAPSSessionHDFSTest extends CAPSTestSuite with MiniDFSClusterFixture with RecordMatchingTestSupport {

  protected override def dfsTestGraphPath = "/csv/sn"

  test("Load graph via DataSource") {
    val testGraphName = GraphName("sn")

    val dataSource = new HdfsCsvPropertyGraphDataSource(
      hadoopConfig = clusterConfig,
      rootPath = "/csv")

    val graph = dataSource.graph(testGraphName)
    graph.nodes("n").asCaps.toDF().collect().toBag should equal(dfsTestGraphNodes)
    graph.relationships("rel").asCaps.toDF().collect.toBag should equal(dfsTestGraphRels)
  }

  test("Load graph via Catalog") {

    val testNamespace = Namespace("myHDFS")
    val testGraphName = GraphName("sn")

    val dataSource = new HdfsCsvPropertyGraphDataSource(
      hadoopConfig = sparkSession.sparkContext.hadoopConfiguration,
      rootPath = "/csv")

    caps.register(testNamespace, dataSource)

    val nodes = caps.cypher(s"FROM GRAPH AT '${testNamespace.value}.${testGraphName.value}' MATCH (n) RETURN n")

    val stefanNode = CAPSNode(1L, Set("Employee", "German", "Person"), CypherMap("name" -> "Stefan", "luckyNumber" -> 42, "languages" -> List("german", "english")))
    val matsNode = CAPSNode(2L, Set("Employee", "Swede", "Person"), CypherMap("name" -> "Mats", "luckyNumber" -> 23, "languages" -> List("swedish", "english", "german")))
    val martinNode = CAPSNode(3L, Set("Employee", "German", "Person"), CypherMap("name" -> "Martin", "luckyNumber" -> 1337, "languages" -> List("german", "english")))
    val maxNode = CAPSNode(4L, Set("Employee", "German", "Person"), CypherMap("name" -> "Max", "luckyNumber" -> 8, "languages" -> List("german", "swedish", "english")))

    nodes.records.iterator.toBag should equal(Bag(
      CypherMap("n" -> stefanNode),
      CypherMap("n" -> matsNode),
      CypherMap("n" -> martinNode),
      CypherMap("n" -> maxNode)
    ))

    val edges = caps.cypher(s"FROM GRAPH AT '${testNamespace.value}.${testGraphName.value}' MATCH ()-[r]->() RETURN r")
    edges.records.asCaps.compact.toMaps should equal(
      Bag(
        CypherMap("r" -> 10L),
        CypherMap("r" -> 20L),
        CypherMap("r" -> 30L)
      ))
  }
}
