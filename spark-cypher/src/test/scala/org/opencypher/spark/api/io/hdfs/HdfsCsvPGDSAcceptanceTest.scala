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
package org.opencypher.spark.api.io.hdfs

import java.nio.file.{Path, Paths}

import org.junit.rules.TemporaryFolder
import org.opencypher.okapi.api.graph.{CypherSession, GraphName}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.testing.PGDSAcceptance
import org.opencypher.okapi.testing.propertygraph.TestGraph
import org.opencypher.spark.impl.io.hdfs.CsvGraphWriter
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.fixture.MiniDFSClusterFixture
import org.opencypher.spark.test.support.creation.caps.CAPSScanGraphFactory

class HdfsCsvPGDSAcceptanceTest extends CAPSTestSuite with MiniDFSClusterFixture with PGDSAcceptance {

  val tempDir = new TemporaryFolder()

  def dsRoot: Path = Paths.get(tempDir.getRoot.getAbsolutePath)

  def graphPath: Path = Paths.get(dsRoot.toString, gn.value)

  override def initSession(): CypherSession = caps

  override def fsTestGraphPath = Some(graphPath.toString)

  override def dfsTestGraphPath = Some(graphPath.toString)

  override def afterAll(): Unit = {
    tempDir.delete()
    super.beforeAll()
  }

  override def create(graphName: GraphName, testGraph: TestGraph, createStatements: String): PropertyGraphDataSource = {
    tempDir.create()
    val propertyGraph = CAPSScanGraphFactory(testGraph)
    CsvGraphWriter(propertyGraph, graphPath.toUri).store()
    HdfsCsvGraphDataSource(clusterConfig, dsRoot.toString)
  }
}
