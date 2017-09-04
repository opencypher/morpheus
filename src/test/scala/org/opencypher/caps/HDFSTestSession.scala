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
package org.opencypher.caps

import java.net.URI

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfterAll, FunSuite}

object HDFSTestSession {

  trait Fixture extends BeforeAndAfterAll {

    self: SparkTestSession.Fixture with FunSuite =>

    private var dfsCluster: MiniDFSCluster = _

    private val testGraphPath = "/csv_graph"

    def hdfsURI: URI = URI.create(s"hdfs+csv://${dfsCluster.getNameNode.getHostAndPort}$testGraphPath")

    override def beforeAll: Unit = {
      dfsCluster = new MiniDFSCluster.Builder(session.sparkContext.hadoopConfiguration).build()
      dfsCluster.waitClusterUp()
      dfsCluster.getFileSystem.copyFromLocalFile(
        new Path(getClass.getResource(testGraphPath).toString),
        new Path(testGraphPath))
    }

    override def afterAll: Unit = {
      dfsCluster.shutdown(true)
    }

    /**
      * Returns the expected nodes for the test graph in /resources/csv_graph
      *
      * @return expected nodes
      */
    def testGraphNodes: Set[Row] = Set(
      Row(1, true,  true, false, true,  "Stefan",   42),
      Row(2, false, true,  true, true,    "Mats",   23),
      Row(3, true,  true, false, true,  "Martin", 1337),
      Row(4, true,  true, false, true,     "Max",    8)
    )

    /**
      * Returns the expected rels for the test graph in /resources/csv_graph
      *
      * @return expected rels
      */
    def testGraphRels: Set[Row] = Set(
      Row(1, 10, 0, 2, 2016),
      Row(2, 20, 0, 3, 2017),
      Row(3, 30, 0, 4, 2015)
    )
  }
}
