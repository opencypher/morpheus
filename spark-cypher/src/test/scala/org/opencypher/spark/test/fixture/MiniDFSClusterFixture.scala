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
package org.opencypher.spark.test.fixture

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.test.fixture.BaseTestFixture
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.GlobalMiniDFSCluster.{clusterForPath, releaseClusterForPath}

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

  protected def hdfsURI: URI = URI.create(s"hdfs://${cluster.getNameNode.getHostAndPort}$dfsTestGraphPath")

  protected def clusterConfig: Configuration = {
    sparkSession.sparkContext.hadoopConfiguration.set("fs.default.name", hdfsURI.toString)
    sparkSession.sparkContext.hadoopConfiguration
  }

  abstract override def afterAll: Unit = {
    releaseClusterForPath(dfsTestGraphPath)
    super.afterAll()
  }
}
