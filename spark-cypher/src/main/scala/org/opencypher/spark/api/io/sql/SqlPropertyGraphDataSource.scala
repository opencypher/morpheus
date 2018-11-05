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
package org.opencypher.spark.api.io.sql

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.impl.exception.{GraphNotFoundException, IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.AbstractPropertyGraphDataSource._
import org.opencypher.spark.api.io.GraphEntity.sourceIdKey
import org.opencypher.spark.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.spark.api.io._
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.sql.ddl.GraphDdl.PropertyMappings
import org.opencypher.sql.ddl._

case class SqlPropertyGraphDataSource(
  graphDdl: GraphDdl,
  sqlDataSourceConfigs: List[SqlDataSourceConfig]
)(implicit val caps: CAPSSession) extends CAPSPropertyGraphDataSource {

  override def hasGraph(graphName: GraphName): Boolean = graphDdl.graphs.contains(graphName)

  override def graph(graphName: GraphName): PropertyGraph = {

    val ddlGraph = graphDdl.graphs.getOrElse(graphName, throw GraphNotFoundException(s"Graph $graphName not found"))
    val capsSchema = ddlGraph.graphType

    // Build CAPS node tables
    val (nodeViewKeys, nodeDfs) = ddlGraph.nodeToViewMappings.mapValues(nvm => readSqlTable(nvm.view)).unzip
    val nodeDataFramesWithIds = nodeViewKeys.zip(addUniqueIds(nodeDfs.toSeq, sourceIdKey)).toMap
    val nodeTables = nodeDataFramesWithIds.map {
      case (nodeViewKey, nodeDf) =>
        val nodeType = nodeViewKey.nodeType
        val columnsWithType = nodeColsWithCypherType(capsSchema, nodeType)
        val inputNodeMapping = createNodeMapping(nodeType, ddlGraph.nodeToViewMappings(nodeViewKey).propertyMappings)
        val normalizedDf = normalizeDataFrame(nodeDf, inputNodeMapping)
        val normalizedMapping = normalizeNodeMapping(inputNodeMapping)

        val validatedDf = normalizedDf
          .validateColumnTypes(columnsWithType)
          .setNullability(columnsWithType)

        CAPSNodeTable.fromMapping(normalizedMapping, validatedDf)
    }.toSeq

    // Build CAPS relationship tables
    val (relViewKeys, relDfs) = ddlGraph.edgeToViewMappings.map(evm => evm.key -> readSqlTable(evm.view)).unzip
    val relDataFramesWithIds = relViewKeys.zip(addUniqueIds(relDfs, sourceIdKey)).toMap

    val relationshipTables = ddlGraph.edgeToViewMappings.map { edgeToViewMapping =>
      val edgeViewKey = edgeToViewMapping.key
      val relType = edgeViewKey.edgeType.head
      val relDf = relDataFramesWithIds(edgeViewKey)
      val startNodeDf = nodeDataFramesWithIds(edgeToViewMapping.startNode.nodeViewKey)
      val endNodeDf = nodeDataFramesWithIds(edgeToViewMapping.endNode.nodeViewKey)

      val relsWithStartNodeId = joinNodeAndEdgeDf(startNodeDf, relDf, edgeToViewMapping.startNode.joinPredicates, sourceStartNodeKey)
      val relsWithEndNodeId = joinNodeAndEdgeDf(endNodeDf, relsWithStartNodeId, edgeToViewMapping.endNode.joinPredicates, sourceEndNodeKey)

      val columnsWithType = relColsWithCypherType(capsSchema, relType)
      val inputRelMapping = createRelationshipMapping(relType, edgeToViewMapping.propertyMappings)
      val normalizedDf = normalizeDataFrame(relsWithEndNodeId, inputRelMapping)
      val normalizedMapping = normalizeRelationshipMapping(inputRelMapping)

      val validatedDf = normalizedDf
        .validateColumnTypes(columnsWithType)
        .setNullability(columnsWithType)

      CAPSRelationshipTable.fromMapping(normalizedMapping, validatedDf)
    }

    caps.graphs.create(nodeTables.head, nodeTables.tail ++ relationshipTables: _*)
  }

  private def joinNodeAndEdgeDf(
    nodeDf: DataFrame,
    edgeDf: DataFrame,
    joinColumns: List[Join],
    newNodeIdColumn: String
  ): DataFrame = {

    val nodePrefix = "node_"
    val edgePrefix = "edge_"

    // to avoid collisions on column names
    val namespacedNodeDf = nodeDf.prefixColumns(nodePrefix)
    val namespacedEdgeDf = edgeDf.prefixColumns(edgePrefix)

    val namespacedJoinColumns = joinColumns
      .map(join => Join(nodePrefix + join.nodeColumn.toPropertyColumnName, edgePrefix + join.edgeColumn.toPropertyColumnName))

    val joinPredicate = namespacedJoinColumns
      .map(join => namespacedNodeDf.col(join.nodeColumn) === namespacedEdgeDf.col(join.edgeColumn))
      .reduce(_ && _)

    val nodeIdColumnName = nodePrefix + sourceIdKey

    // attach id from nodes as start/end by joining on the given columns
    val edgeDfWithNodesJoined = namespacedNodeDf
      .select(nodeIdColumnName, namespacedJoinColumns.map(_.nodeColumn): _*)
      .withColumnRenamed(nodeIdColumnName, newNodeIdColumn)
      .join(namespacedEdgeDf, joinPredicate)

    // drop unneeded node columns (those we joined on) and drop namespace on edge columns
    edgeDfWithNodesJoined.columns.foldLeft(edgeDfWithNodesJoined) {
      case (currentDf, columnName) if columnName.startsWith(nodePrefix) =>
        currentDf.drop(columnName)
      case (currentDf, columnName) if columnName.startsWith(edgePrefix) =>
        currentDf.withColumnRenamed(columnName, columnName.substring(edgePrefix.length))
      case (currentDf, _) =>
        currentDf
    }
  }

  private def readSqlTable(qualifiedViewId: QualifiedViewId): DataFrame = {
    val spark = caps.sparkSession

    val sqlDataSourceConfig = sqlDataSourceConfigs.find(_.dataSourceName == qualifiedViewId.dataSource).get
    val tableName = qualifiedViewId.schema + "." + qualifiedViewId.view

    val inputTable = sqlDataSourceConfig.storageFormat match {
      case JdbcFormat =>
        spark.read
          .format("jdbc")
          .option("url", sqlDataSourceConfig.jdbcUri.getOrElse(throw SqlDataSourceConfigException("Missing JDBC URI")))
          .option("driver", sqlDataSourceConfig.jdbcDriver.getOrElse(throw SqlDataSourceConfigException("Missing JDBC Driver")))
          .option("fetchSize", sqlDataSourceConfig.jdbcFetchSize)
          .option("dbtable", tableName)
          .load()

      case HiveFormat =>

        spark.table(tableName)

      case otherFormat => notFound(otherFormat, Seq(JdbcFormat, HiveFormat))
    }

    inputTable.withPropertyColumns
  }

  private def normalizeDataFrame(dataFrame: DataFrame, mapping: EntityMapping): DataFrame = {
    val dfColumns = dataFrame.schema.fieldNames.map(_.toLowerCase).toSet

    mapping.propertyMapping.foldLeft(dataFrame) {
      case (currentDf, (property, column)) if dfColumns.contains(column.toLowerCase) =>
        currentDf.withColumnRenamed(column, property.toPropertyColumnName)
      case (_, (_, column)) => throw IllegalArgumentException(
        expected = s"Column with name $column",
        actual = dfColumns
      )
    }
  }

  private def normalizeNodeMapping(mapping: NodeMapping): NodeMapping = {
    createNodeMapping(mapping.impliedLabels, mapping.propertyMapping.keys.map(key => key -> key).toMap)
  }

  private def normalizeRelationshipMapping(mapping: RelationshipMapping): RelationshipMapping = {
    createRelationshipMapping(mapping.relTypeOrSourceRelTypeKey.left.get, mapping.propertyMapping.keys.map(key => key -> key).toMap)
  }

  private def createNodeMapping(labelCombination: Set[String], propertyMappings: PropertyMappings): NodeMapping = {
    val initialNodeMapping = NodeMapping.on(sourceIdKey).withImpliedLabels(labelCombination.toSeq: _*)
    propertyMappings.foldLeft(initialNodeMapping) {
      case (currentNodeMapping, (propertyKey, columnName)) =>
        currentNodeMapping.withPropertyKey(propertyKey -> columnName.toPropertyColumnName)
    }
  }

  private def createRelationshipMapping(
    relType: String,
    propertyMappings: PropertyMappings
  ): RelationshipMapping = {
    val initialRelMapping = RelationshipMapping.on(sourceIdKey)
      .withSourceStartNodeKey(sourceStartNodeKey)
      .withSourceEndNodeKey(sourceEndNodeKey)
      .withRelType(relType)
    propertyMappings.foldLeft(initialRelMapping) {
      case (currentRelMapping, (propertyKey, columnName)) =>
        currentRelMapping.withPropertyKey(propertyKey -> columnName.toPropertyColumnName)
    }
  }

  override def schema(name: GraphName): Option[CAPSSchema] = graphDdl.graphs.get(name).map(_.graphType.asCaps)

  override def store(name: GraphName, graph: PropertyGraph): Unit = unsupported("storing a graph")

  override def delete(name: GraphName): Unit = unsupported("deleting a graph")

  override def graphNames: Set[GraphName] = graphDdl.graphs.keySet

  private val className = getClass.getSimpleName

  private def unsupported(operation: String): Nothing =
    throw UnsupportedOperationException(s"$className does not allow $operation")

  private def notFound(needle: Any, haystack: Traversable[Any] = Traversable.empty): Nothing =
    throw IllegalArgumentException(
      expected = if (haystack.nonEmpty) s"one of ${stringList(haystack)}" else "",
      actual = needle
    )

  private def stringList(elems: Traversable[Any]): String =
    elems.mkString("[", ",", "]")
}
