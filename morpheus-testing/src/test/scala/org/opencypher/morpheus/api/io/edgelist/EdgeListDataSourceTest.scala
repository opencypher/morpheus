/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.morpheus.api.io.edgelist

import java.io.{File, PrintWriter}

import org.opencypher.morpheus.testing.MorpheusTestSuite
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.impl.exception
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.mockito.MockitoSugar

class EdgeListDataSourceTest extends MorpheusTestSuite with BeforeAndAfterAll with MockitoSugar {

  private val edgeList: String =
    s"""
       |0 1
       |0 2
       |1 2
       |1 3
     """.stripMargin

  private val tempFile = File.createTempFile(s"morpheus_edgelist_${System.currentTimeMillis()}", "edgelist")

  private val dataSource = EdgeListDataSource(
    tempFile.getAbsolutePath,
    Map("delimiter" -> " "))

  it("should return a static schema") {
    dataSource.schema(EdgeListDataSource.GRAPH_NAME) should equal(Some(EdgeListDataSource.SCHEMA))
  }

  it("should contain only one graph named 'graph'") {
    dataSource.hasGraph(EdgeListDataSource.GRAPH_NAME) should equal(true)
    dataSource.hasGraph(GraphName("foo")) should equal(false)
  }

  it("should throw when trying to delete") {
    a[exception.UnsupportedOperationException] shouldBe thrownBy {
      dataSource.delete(EdgeListDataSource.GRAPH_NAME)
    }
  }

  it("should have only one graph name") {
    dataSource.graphNames should equal(Set(EdgeListDataSource.GRAPH_NAME))
  }

  it("should throw when trying to store a graph") {
    a[exception.UnsupportedOperationException] shouldBe thrownBy {
      dataSource.store(GraphName("foo"), mock[PropertyGraph])
    }
  }

  it("should return the test graph") {
    val graph = dataSource.graph(EdgeListDataSource.GRAPH_NAME)
    graph.nodes("n").size should equal(4)
    graph.relationships("r").size should equal(4)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    new PrintWriter(tempFile.getAbsolutePath) { write(edgeList); close() }
  }

  override protected def afterAll(): Unit = {
    tempFile.delete()
    super.afterAll()
  }
}
