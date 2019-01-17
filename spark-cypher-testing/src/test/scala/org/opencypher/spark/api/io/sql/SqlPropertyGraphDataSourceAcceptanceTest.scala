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
package org.opencypher.spark.api.io.sql

import org.apache.spark.sql.DataFrame
import org.opencypher.graphddl.GraphDdl
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.testing.propertygraph.{CreateGraphFactory, InMemoryTestGraph}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.DefaultGraphDirectoryStructure.nodeTableDirectoryName
import org.opencypher.spark.api.io.sql.IdGenerationStrategy.IdGenerationStrategy
import org.opencypher.spark.api.io.util.CAPSGraphExport._
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.api.io.CAPSPGDSAcceptance
import org.opencypher.spark.testing.support.creation.caps.CAPSScanGraphFactory

import scala.io.Source

// TODO: Replicates/translates many choices in PGDSAcceptance, which makes it brittle and maintenance-intensive
abstract class SqlPropertyGraphDataSourceAcceptanceTest extends CAPSTestSuite with CAPSPGDSAcceptance {

  def sqlDataSourceConfig: SqlDataSourceConfig

  def idGenerationStrategy: IdGenerationStrategy

  def writeTable(df: DataFrame, tableName: String): Unit

  override def initSession(): CAPSSession = caps

  val dataSourceName = "DS"

  val databaseName = "SQLPGDS"

  override def create(
    graphName: GraphName,
    testGraph: InMemoryTestGraph,
    createStatements: String
  ): PropertyGraphDataSource = {

    val scanGraph = CAPSScanGraphFactory(testGraph)
    val schema = scanGraph.schema

    val ddlString = Source
      .fromURI(getClass.getResource("/sql/SqlPropertyGraphDataSourceAcceptanceTest.ddl").toURI)
      .getLines()
      .mkString(System.lineSeparator())

    val graphDdl = GraphDdl(ddlString)

    schema.labelCombinations.combos.foreach { labelCombination =>
      val nodeDf = scanGraph.canonicalNodeTable(labelCombination).removePrefix(propertyPrefix)
      val tableName = databaseName + "." + nodeTableDirectoryName(labelCombination)
      writeTable(nodeDf, tableName)
    }

    schema.relationshipTypes.foreach { relType =>
      val relDf = scanGraph.canonicalRelationshipTable(relType).removePrefix(propertyPrefix)
      val tableName = databaseName + "." + relType
      writeTable(relDf, tableName)
    }

    val sqlPgds = SqlPropertyGraphDataSource(graphDdl, List(sqlDataSourceConfig))

    // The DDL cannot represent nodes without labels, so we need to wrap the data source and add this node manually
    val nodesWithoutLabels = CAPSScanGraphFactory(CreateGraphFactory(createStatementsForNodesWithoutLabels))
    new CAPSPropertyGraphDataSource {
      override def hasGraph(name: GraphName): Boolean = sqlPgds.hasGraph(name)
      override def graph(name: GraphName): PropertyGraph = {
        val sqlGraph = sqlPgds.graph(name)
        if (name == graphName) {
          sqlGraph.unionAll(nodesWithoutLabels)
        } else {
          sqlGraph
        }
      }
      override def schema(name: GraphName): Option[Schema] = {
        val sqlSchema = sqlPgds.schema(name)
        if (name == graphName) {
          sqlSchema.map(_ ++ nodesWithoutLabels.schema)
        } else {
          sqlSchema
        }
      }
      override def store(name: GraphName, graph: PropertyGraph): Unit = sqlPgds.store(name, graph)
      override def delete(name: GraphName): Unit = sqlPgds.delete(name)
      override def graphNames: Set[GraphName] = sqlPgds.graphNames
    }
  }
}
