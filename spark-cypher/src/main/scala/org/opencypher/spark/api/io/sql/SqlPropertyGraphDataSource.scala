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

import org.apache.spark.sql.functions
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, HiveFormat, JdbcFormat}
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.sql.ddl.{DdlDefinitions, NodeMappingDefinition}

case class DDLFormatException(message: String) extends RuntimeException

case class SqlPropertyGraphDataSource(
  ddl: DdlDefinitions,
  sqlDataSources: Map[String, SqlDataSourceConfig]
)(implicit val caps: CAPSSession) extends CAPSPropertyGraphDataSource {

  override def hasGraph(graphName: GraphName): Boolean = ddl.graphByName.contains(graphName.value)

  override def graph(graphName: GraphName): PropertyGraph = {
    val graphSchema = schema(graphName).getOrElse(notFound(s"schema for graph $graphName", ddl.graphSchemas.keySet) )

    val (dataSourceName, databaseName) = ddl.setSchema match {
      case dsName :: dbName :: Nil => dsName -> dbName
      case dsName :: Nil => dsName -> sqlDataSources
        .getOrElse(dsName, notFound(dsName, sqlDataSources.keys))
        .defaultSchema.getOrElse(notFound(s"database schema for $dsName"))
      case Nil => throw DDLFormatException("Missing in DDL: `SET SCHEMA <dataSourceName>.<databaseSchemaName>`")
      case _ => throw DDLFormatException("Illegal DDL format: expected `SET SCHEMA <dataSourceName>.<databaseSchemaName>`")
    }

    val sqlDataSourceConfig = sqlDataSources.getOrElse(dataSourceName, throw SqlDataSourceConfigException(s"No configuration for $dataSourceName"))

    val graphDefinition = ddl.graphByName(graphName.value)

    val nodeMappings: Map[Set[String], NodeMappingDefinition] = graphDefinition.nodeMappings.map(nm => nm.labelNames -> nm).toMap

    val nodeTables = graphSchema.labelCombinations.combos.map(combo => readNodeTable(combo, nodeMappings(combo), graphSchema, sqlDataSourceConfig)).toSeq

    caps.graphs.create(nodeTables.head, nodeTables.tail: _*)
  }

  private def readNodeTable(labelCombination: Set[String], nodeMappingDefinition: NodeMappingDefinition, graphSchema: Schema, sqlDataSourceConfig: SqlDataSourceConfig): CAPSNodeTable = {
    // TODO: getOrElse throw | empty node table
    val tableName = nodeMappingDefinition.viewName

    val propertyToColumnMapping = nodeMappingDefinition.maybePropertyMapping match {
      case Some(propertyToColumnMappingDefinition) => propertyToColumnMappingDefinition
        // for properties, column name is assumed to be the same as the property name
      case None => graphSchema.nodePropertyKeys(labelCombination).map { case (key, _) => key -> key }
    }

    val spark = caps.sparkSession

    val inputTable = sqlDataSourceConfig.storageFormat match {
      case JdbcFormat =>
        spark.read
          .format("jdbc")
          .option("url", sqlDataSourceConfig.jdbcUri.getOrElse(throw SqlDataSourceConfigException("Missing JDBC URI")))
          .option("dbtable", tableName)
          .option("driver", sqlDataSourceConfig.jdbcDriver.getOrElse(throw SqlDataSourceConfigException("Missing JDBC Driver")))
          .option("fetchSize", sqlDataSourceConfig.jdbcFetchSize)
          .load()

      case HiveFormat => spark.table(tableName)

      case otherFormat => notFound(otherFormat, Seq(JdbcFormat, HiveFormat))
    }

    val initialNodeMapping: NodeMapping = NodeMapping.on("id").withImpliedLabels(labelCombination.toSeq: _*)
    val nodeMapping = propertyToColumnMapping.foldLeft(initialNodeMapping) {
      case (currentNodeMapping, (propertyKey, columnName)) => currentNodeMapping.withPropertyKey(propertyKey -> columnName)
    }

    val nodeDf = inputTable.withColumn("id", functions.monotonically_increasing_id())

    CAPSNodeTable.fromMapping(nodeMapping, nodeDf)
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
