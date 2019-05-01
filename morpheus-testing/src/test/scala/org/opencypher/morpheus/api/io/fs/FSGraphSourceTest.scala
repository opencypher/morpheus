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
package org.opencypher.morpheus.api.io.fs

import org.apache.spark.sql.{AnalysisException, Row}
import org.junit.rules.TemporaryFolder
import org.opencypher.morpheus.api.GraphSources
import org.opencypher.morpheus.api.io.FileFormat
import org.opencypher.morpheus.api.io.util.HiveTableName
import org.opencypher.morpheus.api.value.MorpheusElement._
import org.opencypher.morpheus.api.value.MorpheusNode
import org.opencypher.morpheus.impl.acceptance.ScanGraphInit
import org.opencypher.morpheus.testing.MorpheusTestSuite
import org.opencypher.okapi.api.graph.{GraphName, Node, Relationship}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.testing.Bag

class FSGraphSourceTest extends MorpheusTestSuite with ScanGraphInit {

  private var tempDir = new TemporaryFolder()

  private val testDatabaseName = "test"

  override protected def beforeEach(): Unit = {
    morpheus.sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $testDatabaseName")
    tempDir.create()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    morpheus.sparkSession.sql(s"DROP DATABASE IF EXISTS $testDatabaseName CASCADE")
    tempDir.delete()
    tempDir = new TemporaryFolder()
    super.afterEach()
  }

  describe("Hive support") {

    val graphName = GraphName("foo")
    val nodeTableName = HiveTableName(testDatabaseName, graphName, Node, Set("L"))
    val relTableName = HiveTableName(testDatabaseName, graphName, Relationship, Set("R"))
    val testGraph = initGraph("CREATE (:L {prop: 'a'})-[:R {prop: 'b'}]->(:L {prop: 'c'})")

    it("writes nodes and relationships to hive tables") {
      val given = testGraph

      val fs = new FSGraphSource("file:///" + tempDir.getRoot.getAbsolutePath.replace("\\", "/"),
        FileFormat.parquet, Some(testDatabaseName), None)
      fs.store(graphName, given)

      val nodeResult = morpheus.sparkSession.sql(s"SELECT * FROM $nodeTableName")
      nodeResult.collect().toSet should equal(
        Set(
          Row(1.encodeAsMorpheusId, "c"),
          Row(0.encodeAsMorpheusId, "a")
        )
      )

      val relResult = morpheus.sparkSession.sql(s"SELECT * FROM $relTableName")
      relResult.collect().toSet should equal(
        Set(
          Row(2.encodeAsMorpheusId, 0.encodeAsMorpheusId, 1.encodeAsMorpheusId, "b")
        )
      )
    }

    it("deletes the hive database if the graph is deleted") {
      val given = testGraph

      val fs = new FSGraphSource("file:///" + tempDir.getRoot.getAbsolutePath.replace("\\", "/"),
        FileFormat.parquet, Some(testDatabaseName), None)
      fs.store(graphName, given)

      morpheus.sparkSession.sql(s"SELECT * FROM $nodeTableName").collect().toSet should not be empty
      morpheus.sparkSession.sql(s"SELECT * FROM $relTableName").collect().toSet should not be empty

      fs.delete(graphName)
      an [AnalysisException] shouldBe thrownBy {
        morpheus.sparkSession.sql(s"SELECT * FROM $nodeTableName")
      }
      an [AnalysisException] shouldBe thrownBy {
        morpheus.sparkSession.sql(s"SELECT * FROM $relTableName")
      }
    }

  }

  describe("ORC") {
    it("encodes unsupported charaters") {
      val graphName = GraphName("orcGraph")

      val given = initGraph(
        """
          |CREATE (:A {`foo@bar`: 42})
        """.stripMargin)

      val fs = GraphSources.fs("file:///" + tempDir.getRoot.getAbsolutePath.replace("\\", "/")).orc
      fs.store(graphName, given)

      val graph = fs.graph(graphName)

      graph.nodes("n").toMaps should equal(Bag(
        CypherMap("n" -> MorpheusNode(0, Set("A"), CypherMap("foo@bar" -> 42)))
      ))
    }
  }

}
