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
package org.opencypher.spark.api.io.neo4j

import org.junit.rules.TemporaryFolder
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.spark.api.io.neo4j.Neo4jBulkCSVDataSink._
import org.opencypher.spark.impl.acceptance.DefaultGraphInit
import org.opencypher.spark.impl.table.SparkTable
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.TeamDataFixture

import scala.io.Source

class Neo4jBulkCSVDataSinkTest extends CAPSTestSuite with TeamDataFixture with DefaultGraphInit {
  protected val tempDir = new TemporaryFolder()
  tempDir.create()

  val graph: RelationalCypherGraph[SparkTable.DataFrameTable] = initGraph(dataFixture)
  val dataSource = new Neo4jBulkCSVDataSink(tempDir.getRoot.getAbsolutePath)
  private val graphName = GraphName("teamdata")
  dataSource.store(graphName, graph)

  it("writes the correct script file") {
    val root = dataSource.rootPath
    val scriptFilePath = s"$root/${graphName.value}/$SCRIPT_NAME"

    val expected = s"""
                     |#!/bin/sh
                     |if [ $$# -ne 1 ]
                     |then
                     |  echo "Please provide the path to your Neo4j installation (e.g. /usr/share/neo4j/)"
                     |else
                     |  $${1}bin/neo4j-admin import \\
                     |  --database=teamdata \\
                     |  --delimiter="," \\
                     |  --array-delimiter="|" \\
                     |  --id-type=INTEGER \\
                     |  --nodes:Person "$root/teamdata/nodes/Person/schema.csv,$root/teamdata/nodes/Person/part(.*)\\.csv" \\
                     |  --nodes:Person:German "$root/teamdata/nodes/German_Person/schema.csv,$root/teamdata/nodes/German_Person/part(.*)\\.csv" \\
                     |  --nodes:Person:Swede "$root/teamdata/nodes/Person_Swede/schema.csv,$root/teamdata/nodes/Person_Swede/part(.*)\\.csv" \\
                     |  --relationships:KNOWS "$root/teamdata/relationships/KNOWS/schema.csv,$root/teamdata/relationships/KNOWS/part(.*)\\.csv"
                     |fi
                     |""".stripMargin

    Source.fromFile(scriptFilePath).mkString should equal(expected)
  }

  it("writes the correct schema files") {
    Source.fromFile(dataSource.schemaFileForNodes(graphName, Set("Person", "German"))).mkString should equal(
      "___capsID:ID,languages:string[],luckyNumber:int,name:string"
    )

    Source.fromFile(dataSource.schemaFileForNodes(graphName, Set("Person"))).mkString should equal(
      "___capsID:ID,languages:string[],luckyNumber:int,name:string"
    )

    Source.fromFile(dataSource.schemaFileForNodes(graphName, Set("Person", "Swede"))).mkString should equal(
      "___capsID:ID,luckyNumber:int,name:string"
    )

    Source.fromFile(dataSource.schemaFileForRelationships(graphName, "KNOWS")).mkString should equal(
      ":START_ID,:END_ID,since:int"
    )
  }
}
