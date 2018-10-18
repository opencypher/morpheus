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
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, HiveFormat, JdbcFormat}
import org.opencypher.spark.impl.CAPSFunctions.partitioned_id_assignment
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.sql.ddl.{DdlDefinitions, NodeMappingDefinition, SetSchemaDefinition}

case class DDLFormatException(message: String) extends RuntimeException

case class SqlPropertyGraphDataSource(
  ddl: DdlDefinitions,
  sqlDataSources: Map[String, SqlDataSourceConfig]
)(implicit val caps: CAPSSession) extends CAPSPropertyGraphDataSource {

  override def hasGraph(graphName: GraphName): Boolean = ddl.graphByName.contains(graphName.value)

  override def graph(graphName: GraphName): PropertyGraph = {
    val graphSchema = schema(graphName).getOrElse(notFound(s"schema for graph $graphName", ddl.graphSchemas.keySet))

    val (dataSourceName, databaseName) = ddl.setSchema match {
      case Some(SetSchemaDefinition(dsName, Some(dbName))) => dsName -> dbName
      case Some(SetSchemaDefinition(dsName, None)) => dsName -> sqlDataSources
        .getOrElse(dsName, notFound(dsName, sqlDataSources.keys))
        .defaultSchema.getOrElse(notFound(s"database schema for $dsName"))
      case _ => throw DDLFormatException("Missing in DDL: `SET SCHEMA <dataSourceName>.<databaseSchemaName>`")
    }

    val sqlDataSourceConfig = sqlDataSources.getOrElse(dataSourceName, throw SqlDataSourceConfigException(s"No configuration for $dataSourceName"))

    val graphDefinition = ddl.graphByName(graphName.value)

    val comboToNodeMapping: Map[Set[String], NodeMappingDefinition] = graphDefinition.nodeMappings.map(nm => nm.labelNames -> nm).toMap

    // TODO: Add ability to map multiple views/DFs to the same combo
    val nodeDataFramesWithoutIds: Seq[(Set[String], DataFrame)] = graphSchema.labelCombinations.combos.toSeq
      .map(combo => combo -> readNodeTable(combo, comboToNodeMapping(combo).viewName, sqlDataSourceConfig))

    val dfPartitionCounts = nodeDataFramesWithoutIds.map(_._2.rdd.getNumPartitions)
    val dfPartitionStartDeltas = dfPartitionCounts.scan(0)(_ + _).dropRight(1) // drop last delta, as we don't need it

    // TODO: Ensure added id does not collide with a property called `id`
    val nodeDataFramesWithIds: Seq[(Set[String], DataFrame)] = nodeDataFramesWithoutIds.zip(dfPartitionStartDeltas).map {
      case ((combo, df), partitionStartDelta) =>
        val dfWithIdColumn = df.withColumn("id", partitioned_id_assignment(partitionStartDelta))
        combo -> dfWithIdColumn
    }

    val nodeTables: Seq[CAPSNodeTable] = nodeDataFramesWithIds.map { case (combo, df) =>
      val nodeMapping = computeNodeMapping(combo, graphSchema, comboToNodeMapping(combo))
      CAPSNodeTable.fromMapping(nodeMapping, df)
    }

    caps.graphs.create(nodeTables.head, nodeTables.tail: _*)
  }

  private def computeNodeMapping(
    labelCombination: Set[String],
    graphSchema: Schema,
    nodeMappingDefinition: NodeMappingDefinition
  ): NodeMapping = {
    val propertyToColumnMapping = nodeMappingDefinition.maybePropertyMapping match {
      case Some(propertyToColumnMappingDefinition) => propertyToColumnMappingDefinition
      // TODO: support unicode characters in properties and ensure there are no collisions with column name `id`
      case None => graphSchema.nodePropertyKeys(labelCombination).map { case (key, _) => key -> key }
    }
    val initialNodeMapping: NodeMapping = NodeMapping.on("id").withImpliedLabels(labelCombination.toSeq: _*)
    val nodeMapping = propertyToColumnMapping.foldLeft(initialNodeMapping) {
      case (currentNodeMapping, (propertyKey, columnName)) => currentNodeMapping.withPropertyKey(propertyKey -> columnName)
    }
    nodeMapping
  }

  private def readNodeTable(
    labelCombination: Set[String],
    viewName: String,
    sqlDataSourceConfig: SqlDataSourceConfig
  ): DataFrame = {
    val spark = caps.sparkSession

    val inputTable = sqlDataSourceConfig.storageFormat match {
      case JdbcFormat =>
        spark.read
          .format("jdbc")
          .option("url", sqlDataSourceConfig.jdbcUri.getOrElse(throw SqlDataSourceConfigException("Missing JDBC URI")))
          .option("driver", sqlDataSourceConfig.jdbcDriver.getOrElse(throw SqlDataSourceConfigException("Missing JDBC Driver")))
          .option("fetchSize", sqlDataSourceConfig.jdbcFetchSize)
          .option("dbtable", viewName)
          .load()

      case HiveFormat => spark.table(viewName)

      case otherFormat => notFound(otherFormat, Seq(JdbcFormat, HiveFormat))
    }

    inputTable
  }


  override def schema(name: GraphName): Option[Schema] = ddl.graphSchemas.get(name.value)

  override def store(name: GraphName, graph: PropertyGraph): Unit = unsupported("storing a graph")

  override def delete(name: GraphName): Unit = unsupported("deleting a graph")

  override def graphNames: Set[GraphName] = ddl.graphByName.keySet.map(GraphName)

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
