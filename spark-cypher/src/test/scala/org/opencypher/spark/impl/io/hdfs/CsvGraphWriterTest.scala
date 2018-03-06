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
import java.nio.file.Files

import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.{GraphCreationFixture, TeamDataFixture}

class CsvGraphWriterTest extends CAPSTestSuite with TeamDataFixture with GraphCreationFixture {

  it("can store a graph to file system") {
    val tmpPath = Files.createTempDirectory("caps_graph")

    val inputGraph = initGraph(dataFixtureWithoutArrays)
    val fileHandler = new LocalFileHandler(tmpPath.toString)
    new CsvGraphWriter(inputGraph, fileHandler).store()

    // Verification
    val fileURI: URI = new URI(s"file://${tmpPath.toString}")
    val loader = CsvGraphLoader(fileURI.toString, session.sparkContext.hadoopConfiguration)
    val expected: CAPSGraph = loader.load.asCaps
    expected.nodes("n").toDF().collect().toBag should equal(csvTestGraphNodesWithoutArrays)
    expected.relationships("rel").toDF().collect.toBag should equal(csvTestGraphRelsWithoutArrays)
  }
}
