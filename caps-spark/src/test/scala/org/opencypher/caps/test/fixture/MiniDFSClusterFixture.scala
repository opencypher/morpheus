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
package org.opencypher.caps.test.fixture

import java.net.URI

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.spark.sql.{Row, SparkSession}
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.GlobalMiniDFSCluster.{clusterForPath, releaseClusterForPath}

import scala.collection.{Bag, mutable}
import scala.util.Try

/**
  * Allows multiple tests to use the same MiniDFSCluster for the same local filesystem folder.
  */
object GlobalMiniDFSCluster {

  private var clusters: Map[String, MiniDFSCluster] = Map.empty
  private var clusterUsageCountsByPath: Map[String, Int] = Map.empty.withDefaultValue(0)

  def clusterForPath(path: String)(implicit session: SparkSession): MiniDFSCluster = synchronized {
    if (clusters.contains(path)) {
      clusterUsageCountsByPath = clusterUsageCountsByPath.updated(path, clusterUsageCountsByPath(path) + 1)
      clusters(path)
    } else {
      val cluster = new MiniDFSCluster.Builder(session.sparkContext.hadoopConfiguration).build()
      cluster.waitClusterUp()
      cluster.getFileSystem.copyFromLocalFile(
        new Path(getClass.getResource(path).toString),
        new Path(path))
      clusters += path -> cluster
      clusterUsageCountsByPath += path -> 1
      cluster
    }
  }

  def releaseClusterForPath(path: String): Unit = synchronized {

    val count = clusterUsageCountsByPath(path)
    if (count == 1) {
      Try(clusters(path).shutdown(true))
      clusters -= path
      clusterUsageCountsByPath -= path
    } else {
      clusterUsageCountsByPath = clusterUsageCountsByPath.updated(path, clusterUsageCountsByPath(path) - 1)
    }
  }
}

trait MiniDFSClusterFixture extends BaseTestFixture {

  self: SparkSessionFixture with CAPSTestSuite =>

  protected def dfsTestGraphPath: String

  protected val cluster: MiniDFSCluster = clusterForPath(dfsTestGraphPath)

  protected def hdfsURI: URI = {
    hdfsURI(dfsTestGraphPath)
  }

  protected def hdfsURI(path: String): URI = {
    URI.create(s"hdfs://${cluster.getNameNode.getHostAndPort}$path")
  }

  abstract override def afterAll: Unit = {
    releaseClusterForPath(dfsTestGraphPath)
    super.afterAll()
  }

  /**
    * Returns the expected nodes for the test graph in /resources/csv/sn
    *
    * @return expected nodes
    */
  def dfsTestGraphNodes: Bag[Row] = Bag(
    Row(1L, true, true, true, false, mutable.WrappedArray.make(Array("german", "english")), 42L, "Stefan"),
    Row(2L, true, false, true, true, mutable.WrappedArray.make(Array("swedish", "english", "german")), 23L, "Mats"),
    Row(3L, true, true, true, false, mutable.WrappedArray.make(Array("german", "english")), 1337L, "Martin"),
    Row(4L, true, true, true, false, mutable.WrappedArray.make(Array("german", "swedish", "english")), 8L, "Max")
  )

  /**
    * Returns the expected rels for the test graph in /resources/csv/sn
    *
    * @return expected rels
    */
  def dfsTestGraphRels: Bag[Row] = Bag(
    Row(1L, 10L, "KNOWS", 2L, 2016L),
    Row(2L, 20L, "KNOWS", 3L, 2017L),
    Row(3L, 30L, "KNOWS", 4L, 2015L)
  )
}
