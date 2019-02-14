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

import java.net.URI

import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, DataFrameReader, functions}
import org.opencypher.graphddl.GraphDdl.PropertyMappings
import org.opencypher.graphddl._
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.types.{CTDate, CypherType}
import org.opencypher.okapi.impl.exception.{GraphNotFoundException, IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.AbstractPropertyGraphDataSource._
import org.opencypher.spark.api.io.GraphEntity.sourceIdKey
import org.opencypher.spark.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.spark.api.io._
import org.opencypher.spark.api.io.sql.IdGenerationStrategy._
import org.opencypher.spark.api.io.sql.SqlDataSourceConfig.{File, Hive, Jdbc}
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.impl.table.SparkTable._
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

import scala.reflect.io.Path

case class SqlPropertyGraphDataSource(
  graphDdl: GraphDdl,
  sqlDataSourceConfigs: Map[String, SqlDataSourceConfig],
  idGenerationStrategy: IdGenerationStrategy = SerializedId
)(implicit val caps: CAPSSession) extends CAPSPropertyGraphDataSource {

  override def hasGraph(graphName: GraphName): Boolean = graphDdl.graphs.contains(graphName)

  override def graph(graphName: GraphName): PropertyGraph = {

    val ddlGraph = graphDdl.graphs.getOrElse(graphName, throw GraphNotFoundException(s"Graph $graphName not found"))
    val capsSchema = ddlGraph.graphType

    // Build CAPS node tables
    val nodeDataFrames = ddlGraph.nodeToViewMappings.mapValues(nvm => readTable(nvm.view))

    // Generate node identifiers
    val nodeDataFramesWithIds = createIdForTables(nodeDataFrames, ddlGraph, sourceIdKey, idGenerationStrategy)

    val nodeTables = nodeDataFramesWithIds.map {
      case (nodeViewKey, nodeDf) =>
        val nodeElementTypes = nodeViewKey.nodeType.elementTypes
        val columnsWithType = nodeColsWithCypherType(capsSchema, nodeElementTypes)
        val inputNodeMapping = createNodeMapping(nodeElementTypes, ddlGraph.nodeToViewMappings(nodeViewKey).propertyMappings)
        val normalizedDf = normalizeDataFrame(nodeDf, inputNodeMapping, columnsWithType).castToLong
        val normalizedMapping = normalizeNodeMapping(inputNodeMapping)

        normalizedDf.validateColumnTypes(columnsWithType)

        CAPSNodeTable.fromMapping(normalizedMapping, normalizedDf)
    }.toSeq

    // Build CAPS relationship tables
    val relDataFrames = ddlGraph.edgeToViewMappings.map(evm => evm.key -> readTable(evm.view)).toMap

    // Generate relationship identifiers
    val relDataFramesWithIds = createIdForTables(relDataFrames, ddlGraph, sourceIdKey, idGenerationStrategy)

    val relationshipTables = ddlGraph.edgeToViewMappings.map { edgeToViewMapping =>
      val edgeViewKey = edgeToViewMapping.key
      val relElementType = edgeViewKey.relType.elementType
      val relDf = relDataFramesWithIds(edgeViewKey)
      val startNodeViewKey = edgeToViewMapping.startNode.nodeViewKey
      val endNodeViewKey = edgeToViewMapping.endNode.nodeViewKey

      // generate the start/end node id using the same parameters as for the corresponding node table
      val idColumnNamesStartNode = edgeToViewMapping.startNode.joinPredicates.map(_.edgeColumn).map(_.toPropertyColumnName)
      val relsWithStartNodeId = createIdForTable(relDf, startNodeViewKey, idColumnNamesStartNode, sourceStartNodeKey, idGenerationStrategy)
      val idColumnNamesEndNode = edgeToViewMapping.endNode.joinPredicates.map(_.edgeColumn).map(_.toPropertyColumnName)
      val relsWithEndNodeId = createIdForTable(relsWithStartNodeId, endNodeViewKey, idColumnNamesEndNode, sourceEndNodeKey, idGenerationStrategy)

      val columnsWithType = relColsWithCypherType(capsSchema, relElementType)
      val inputRelMapping = createRelationshipMapping(relElementType, edgeToViewMapping.propertyMappings)
      val normalizedDf = normalizeDataFrame(relsWithEndNodeId, inputRelMapping, columnsWithType).castToLong
      val normalizedMapping = normalizeRelationshipMapping(inputRelMapping)

      normalizedDf.validateColumnTypes(columnsWithType)

      CAPSRelationshipTable.fromMapping(normalizedMapping, normalizedDf)
    }

    caps.graphs.create(Some(capsSchema), nodeTables.head, nodeTables.tail ++ relationshipTables: _*)
  }

  private def readTable(viewId: ViewId): DataFrame = {
    val sqlDataSourceConfig = sqlDataSourceConfigs.get(viewId.dataSource) match {
      case None =>
        val knownDataSources = sqlDataSourceConfigs.keys.mkString("'", "';'", "'")
        throw SqlDataSourceConfigException(s"Data source '${viewId.dataSource}' not configured; see data sources configuration. Known data sources: $knownDataSources")
      case Some(config) =>
        config
    }

    val inputTable = sqlDataSourceConfig match {
      case hive@Hive => readSqlTable(viewId, hive)
      case jdbc: Jdbc => readSqlTable(viewId, jdbc)
      case file: File => readFile(viewId, file)
    }

    inputTable.safeRenameColumns(inputTable.columns.map(col => col -> col.toPropertyColumnName).toMap)
  }

  private def readSqlTable(viewId: ViewId, sqlDataSourceConfig: SqlDataSourceConfig) = {
    val spark = caps.sparkSession

    val tableName = (viewId.maybeSetSchema, viewId.parts) match {
      case (_, _ :: schema :: view :: Nil) => s"$schema.$view"
      case (Some(SetSchemaDefinition(dataSource, schema)), view :: Nil) => s"$schema.$view"
      case (None, view) if view.size < 3 =>
        malformed("Relative view identifier requires a preceding SET SCHEMA statement", view.mkString("."))
      case (Some(_), view) if view.size > 1 =>
        malformed("Relative view identifier must have exactly one segment", view.mkString("."))
    }

    implicit class DataFrameReaderOps(read: DataFrameReader) {
      def maybeOption(key: String, value: Option[String]): DataFrameReader =
        value.fold(read)(read.option(key, _))
    }

    sqlDataSourceConfig match {
      case Jdbc(url, driver, options) =>
        spark.read
          .format("jdbc")
          .option("url", url)
          .option("driver", driver)
          .option("fetchSize", "100") // default value
          .options(options)
          .option("dbtable", tableName)
          .load()

      case SqlDataSourceConfig.Hive =>
        spark.table(tableName)

      case otherFormat => notFound(otherFormat, Seq(JdbcFormat, HiveFormat))
    }
  }

  private def readFile(viewId: ViewId, dataSourceConfig: File): DataFrame = {
    val spark = caps.sparkSession

    val optionsByFormat: Map[StorageFormat, Map[String, String]] = Map(
      FileFormat.csv -> Map("header" -> "true", "inferSchema" -> "true")
    )

    val viewPath = viewId.parts.lastOption.getOrElse(
      malformed("File names must be defined with the data source", viewId.parts.mkString(".")))

    val filePath = if (new URI(viewPath).isAbsolute) {
      viewPath
    } else {
      dataSourceConfig.basePath match {
        case Some(rootPath) => (Path(rootPath) / Path(viewPath)).toString()
        case None => unsupported("Relative view file names require basePath to be set")
      }
    }

    spark.read
      .format(dataSourceConfig.format.name)
      .options(optionsByFormat.getOrElse(dataSourceConfig.format, Map.empty))
      .options(dataSourceConfig.options)
      .load(filePath.toString)
  }

  private def normalizeDataFrame(
    dataFrame: DataFrame,
    mapping: EntityMapping,
    columnTypes: Map[String, CypherType]
  ): DataFrame = {
    val fields = dataFrame.schema.fields
    val indexedFields = fields.map(field => field.name.toLowerCase).zipWithIndex.toMap

    val columnRenamings = mapping.propertyMapping.map {
      case (property, column) if indexedFields.contains(column.toLowerCase) =>
        fields(indexedFields(column.toLowerCase)).name -> property.toPropertyColumnName
      case (_, column) => throw IllegalArgumentException(
        expected = s"Column with name $column",
        actual = indexedFields)
    }
    val renamedDf = dataFrame.safeRenameColumns(columnRenamings)
    normalizeTemporalColumns(renamedDf, columnTypes)
  }

  private def normalizeTemporalColumns(df: DataFrame, columnTypes: Map[String, CypherType]): DataFrame = {
    columnTypes
      .mapValues(_.material)
      .collect { case (column, _@CTDate) => column }
      .foldLeft(df) { case (acc, dateCol) => acc.withColumn(dateCol, acc.col(dateCol).cast(DateType)) }
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

  /**
    * Creates a 64-bit identifier for each row in the given input table. The identifier is computed by hashing or
    * serializing (depending on the strategy) the view name, the element type (i.e. its labels) and the values stored in a
    * given set of columns.
    *
    * @param dataFrame      input table / view
    * @param elementViewKey node / edge view key used for hashing
    * @param idColumnNames  columns used for hashing
    * @param newIdColumn    name of the new id column
    * @tparam T node / edge view key
    * @return input table / view with an additional column that contains potentially unique identifiers
    */
  private def createIdForTable[T <: ElementViewKey](
    dataFrame: DataFrame,
    elementViewKey: T,
    idColumnNames: List[String],
    newIdColumn: String,
    strategy: IdGenerationStrategy
  ): DataFrame = {
    val viewLiteral = functions.lit(elementViewKey.viewId.parts.mkString("."))
    val elementTypeLiterals = elementViewKey.elementType.toSeq.sorted.map(functions.lit)
    val idColumns = idColumnNames.map(dataFrame.col)
    strategy match {
      case HashBasedId => dataFrame.withHashColumn(Seq(viewLiteral) ++ elementTypeLiterals ++ idColumns, newIdColumn)
      case SerializedId => dataFrame.withSerializedIdColumn(Seq(viewLiteral) ++ elementTypeLiterals ++ idColumns, newIdColumn)
    }
  }

  /**
    * Creates a 64-bit identifier for each row in the given input tables. The identifier is computed by hashing or
    * serializing a specific set of columns of the input table. For node tables, we either pick the the join columns
    * from the relationship mappings (i.e. the columns we join on) or all columns if the node is unconnected.
    *
    * In order to avoid or reduce the probability of ID collisions (depending on the strategy), the view name and the
    * element type (i.e. its labels) are additional input for the hash function and ID serializer.
    *
    * @param views       input tables
    * @param ddlGraph    DDL graph instance definition
    * @param newIdColumn name of the new id column
    * @tparam T node / edge view key
    * @return input tables with an additional column that contains potentially unique identifiers
    */
  private def createIdForTables[T <: ElementViewKey](
    views: Map[T, DataFrame],
    ddlGraph: Graph,
    newIdColumn: String,
    strategy: IdGenerationStrategy
  ): Map[T, DataFrame] = {
    views.map { case (elementViewKey, dataFrame) =>
      val idColumnNames = elementViewKey match {
        case nvk: NodeViewKey => ddlGraph.nodeIdColumnsFor(nvk) match {
          case Some(columnNames) => columnNames.map(_.toPropertyColumnName)
          case None => dataFrame.columns.toList
        }
        case _: EdgeViewKey => dataFrame.columns.toList
      }
      elementViewKey -> createIdForTable(dataFrame, elementViewKey, idColumnNames, newIdColumn, strategy)
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

  def malformed(desc: String, identifier: String): Nothing =
    throw MalformedIdentifier(s"$desc: $identifier")

  private def stringList(elems: Traversable[Any]): String =
    elems.mkString("[", ",", "]")

}
