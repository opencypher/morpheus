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
package org.opencypher.spark.impl.io.hdfs

import java.net.URI

import org.apache.http.client.utils.URIBuilder
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.{MiniDFSClusterFixture, TeamDataFixture}

class CsvGraphLoaderTest extends CAPSTestSuite
  with MiniDFSClusterFixture
  with TeamDataFixture {

  protected override def dfsTestGraphPath = Some("/csv/sn")

  override protected def hdfsURI: URI = new URIBuilder(super.hdfsURI).setPath(dfsTestGraphPath.get).build()

  test("load csv graph from HDFS") {
    val loader = CsvGraphLoader(hdfsURI, session.sparkContext.hadoopConfiguration)

    val graph: CAPSGraph = loader.load.asCaps
    graph.nodes("n").toDF().collect().toBag should equal(csvTestGraphNodes)
    graph.relationships("rel").toDF().collect.toBag should equal(csvTestGraphRels)
  }

  test("load csv graph from local file") {
    val fileURI: URI = new URI(s"file://${getClass.getResource("/csv/sn").getPath}")
    val loader = CsvGraphLoader(fileURI, session.sparkContext.hadoopConfiguration)

    val graph: CAPSGraph = loader.load.asCaps
    graph.nodes("n").toDF().collect().toBag should equal(csvTestGraphNodes)
    graph.relationships("rel").toDF().collect.toBag should equal(csvTestGraphRels)
  }
}
