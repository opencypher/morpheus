/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.test.fixture

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.http.client.utils.URIBuilder
import org.opencypher.okapi.testing.BaseTestFixture
import org.opencypher.spark.test.CAPSTestSuite

trait MiniDFSClusterFixture extends BaseTestFixture {

  self: SparkSessionFixture with CAPSTestSuite =>

  private val HDFS_URI_SCHEME = "hdfs"

  // Override this to some String to trigger a copy from local FS into HDFS
  // e.g. dfsTestGraph = "/foo" triggers a copy from 'file:/path/to/resources/foo' to 'hdfs://host:port/foo'
  protected def dfsTestGraphPath: Option[String] = None
  // Override this to change the path to the graph in the local file system
  // If None, it is assumed that the local graph is contained in resources/<dfsTestGraphPath>
  protected def fsTestGraphPath: Option[String] = None

  protected lazy val cluster: MiniDFSCluster = {
    val cluster = new MiniDFSCluster.Builder(session.sparkContext.hadoopConfiguration).build()
    cluster.waitClusterUp()

    // copy from local FS to HDFS if necessary
    if (dfsTestGraphPath.isDefined) {
      val dfsPathString = dfsTestGraphPath.get
      val fsPathString = fsTestGraphPath.getOrElse(getClass.getResource(dfsPathString).toString)
      cluster.getFileSystem.copyFromLocalFile(
        new Path(fsPathString),
        new Path(dfsPathString))
    }
    cluster
  }

  protected def hdfsURI: URI = new URIBuilder()
    .setScheme(HDFS_URI_SCHEME)
    .setHost(cluster.getNameNode.getHostAndPort)
    .setPath(dfsTestGraphPath.getOrElse(File.separator))
    .build()

  protected def clusterConfig: Configuration = {
    session.sparkContext.hadoopConfiguration
      .set("fs.default.name", new URIBuilder()
        .setScheme(HDFS_URI_SCHEME)
        .setHost(cluster.getNameNode.getHostAndPort)
        .build()
        .toString
      )
    session.sparkContext.hadoopConfiguration
  }

  abstract override def afterAll: Unit = {
    session.sparkContext.hadoopConfiguration.clear()
    cluster.shutdown(true)
    super.afterAll()
  }
}
