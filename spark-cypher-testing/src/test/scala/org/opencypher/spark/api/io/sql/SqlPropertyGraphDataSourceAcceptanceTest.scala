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
import org.opencypher.graphddl._
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.SchemaPattern
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.testing.propertygraph.InMemoryTestGraph
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.DefaultGraphDirectoryStructure.nodeTableDirectoryName
import org.opencypher.spark.api.io.sql.IdGenerationStrategy.IdGenerationStrategy
import org.opencypher.spark.api.io.sql.util.DdlUtils._
import org.opencypher.spark.api.io.util.CAPSGraphExport._
import org.opencypher.spark.api.io.{GraphEntity, Relationship}
import org.opencypher.spark.impl.table.SparkTable._
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.api.io.CAPSPGDSAcceptance
import org.opencypher.spark.testing.support.creation.caps.CAPSScanGraphFactory

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

    val joinFromStartNode: List[Join] = List(Join(GraphEntity.sourceIdKey, Relationship.sourceStartNodeKey))
    val joinFromEndNode: List[Join] = List(Join(GraphEntity.sourceIdKey, Relationship.sourceEndNodeKey))

    def viewId(labelCombination: Set[String]): ViewId = {
      ViewId(None, List(dataSourceName, databaseName, nodeTableDirectoryName(labelCombination)))
    }

    def nodeViewKey(labelCombination: Set[String]): NodeViewKey = {
      NodeViewKey(NodeType(labelCombination), viewId(labelCombination))
    }

    val nodeToViewMappings: Map[NodeViewKey, NodeToViewMapping] = {
      schema.labelCombinations.combos.map { labelCombination =>
        NodeToViewMapping(
          NodeType(labelCombination),
          viewId(labelCombination),
          schema.nodePropertyKeys(labelCombination).keySet.map(k => k -> k).toMap)
      }.map(mapping => mapping.key -> mapping).toMap
    }

    val edgeToViewMappings: List[EdgeToViewMapping] = {
      val schemaPatterns: Map[String, Set[SchemaPattern]] = scanGraph.schemaPatterns.groupBy(_.relType)
      schemaPatterns.flatMap { case (relType, patterns) =>
        val viewId = ViewId(None, List(dataSourceName, databaseName, relType))
        val relKeyMapping = schema.relationshipPropertyKeys(relType).keySet.map(k => k -> k).toMap
        patterns.map { case SchemaPattern(sourceLabelCombination, _, targetLabelCombination) =>
          EdgeToViewMapping(
            RelationshipType(NodeType(sourceLabelCombination), relType, NodeType(targetLabelCombination)),
            viewId,
            StartNode(nodeViewKey(sourceLabelCombination), joinFromStartNode),
            EndNode(nodeViewKey(targetLabelCombination), joinFromEndNode),
            relKeyMapping)
        }
      }
    }.toList

    val graphDdl = GraphDdl(Map(graphName -> Graph(
      graphName,
      schema,
      nodeToViewMappings,
      edgeToViewMappings
    )))

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

    SqlPropertyGraphDataSource(graphDdl, Map(dataSourceName -> sqlDataSourceConfig))
  }
}
