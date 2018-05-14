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
package org.opencypher.spark.impl.io.hdfs

import java.net.URI
import java.nio.file.Files

import org.apache.spark.sql.Row
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.{GraphConstructionFixture, MiniDFSClusterFixture, TeamDataFixture}

// This tests depends on the id generation in Neo4j (harness)
class CsvGraphWriterLocalFSTest extends CAPSTestSuite with MiniDFSClusterFixture with TeamDataFixture with GraphConstructionFixture {

  it("can store a graph to local file system") {
    val tmpPath = Files.createTempDirectory("caps_graph")

    val inputGraph = initGraph(dataFixtureWithoutArrays)
    val fileHandler = new LocalFileHandler(new URI(tmpPath.toString))
    new CsvGraphWriter(inputGraph, fileHandler).store()

    // Verification
    val fileURI: URI = new URI(s"file://${tmpPath.toString}")
    val loader = CsvGraphLoader(fileURI, session.sparkContext.hadoopConfiguration)
    val expected: CAPSGraph = loader.load.asCaps
    expected.tags should equal(Set(0))
    val expectedNodes = expected.nodes("n").toDF()
    expectedNodes.collect().toBag should equal(csvTestGraphNodesWithoutArrays)
    val expectedRels = expected.relationships("rel").toDF()
    expectedRels.collect.toBag should equal(csvTestGraphRelsWithoutArrays)
  }

  it("can store a graph with no relationships") {
    val tmpPath = Files.createTempDirectory("caps_graph")

    val inputGraph = initGraph("CREATE (a:A)")
    val fileHandler = new LocalFileHandler(new URI(tmpPath.toString))
    new CsvGraphWriter(inputGraph, fileHandler).store()

    // Verification
    val fileURI: URI = new URI(s"file://${tmpPath.toString}")
    val loader = CsvGraphLoader(fileURI, session.sparkContext.hadoopConfiguration)
    val expected: CAPSGraph = loader.load.asCaps
    expected.tags should equal(Set(0))
    val expectedNodes = expected.nodes("n").toDF()
    expectedNodes.collect().toBag should equal(Bag(Row(0L, true)))

    expected.relationships("rel").size should equal(0)
  }

}
