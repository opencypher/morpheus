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
package org.opencypher.caps.test.fixture

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.spark.sql.Row
import org.opencypher.caps.test.BaseTestSuite

trait MiniDFSClusterFixture extends BaseTestFixture {

  self: SparkSessionFixture with BaseTestSuite =>

  protected var dfsCluster: MiniDFSCluster = _

  protected val dfsTestGraphPath = "/csv_graph"

  abstract override def beforeAll(): Unit = {
    super.beforeAll()
    dfsCluster = new MiniDFSCluster.Builder(session.sparkContext.hadoopConfiguration).build()
    dfsCluster.waitClusterUp()
    dfsCluster.getFileSystem.copyFromLocalFile(
      new Path(getClass.getResource(dfsTestGraphPath).toString),
      new Path(dfsTestGraphPath))
  }

  abstract override def afterAll: Unit = {
    dfsCluster.shutdown(true)
    super.afterAll()
  }

  /**
    * Returns the expected nodes for the test graph in /resources/csv_graph
    *
    * @return expected nodes
    */
  def dfsTestGraphNodes: Set[Row] = Set(
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
  def dfsTestGraphRels: Set[Row] = Set(
    Row(1, 10, 0, 2, 2016),
    Row(2, 20, 0, 3, 2017),
    Row(3, 30, 0, 4, 2015)
  )
}
