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
import org.opencypher.sql.ddl.{DdlDefinitions, NodeMappingDefinition, NodeToViewDefinition, SetSchemaDefinition}

case class DDLFormatException(message: String) extends RuntimeException

/**
  * Id of a SQL view assigned to a specific label definition. This is necessary because the same SQL view can be
  * assigned to multiple label definitions.
  */
// TODO: move to IR
case class AssignedViewIdentifier(labelDefinitions: Set[String], viewName: String)

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

    // Node tables

    val nodeDataFramesWithoutIds = for {
      nodeMapping <- graphDefinition.nodeMappings
      nodeToViewDefinition <- nodeMapping.nodeToViewDefinitions
    } yield AssignedViewIdentifier(nodeMapping.labelNames, nodeToViewDefinition.viewName) -> readSqlTable(nodeToViewDefinition.viewName, sqlDataSourceConfig)

    // id assignment preparation
    val dfPartitionCounts = nodeDataFramesWithoutIds.map(_._2.rdd.getNumPartitions)
    val dfPartitionStartDeltas = dfPartitionCounts.scan(0)(_ + _).dropRight(1) // drop last delta, as we don't need it

    // actual id assignment
    // TODO: Ensure added id does not collide with a property called `id`
    val nodeDataFramesWithIds = nodeDataFramesWithoutIds.zip(dfPartitionStartDeltas).map {
      case ((instantiatedViewIdentifier, df), partitionStartDelta) =>
        val dfWithIdColumn = df.withColumn("id", partitioned_id_assignment(partitionStartDelta))
        instantiatedViewIdentifier -> dfWithIdColumn
    }.toMap


    // TODO: maps a AssignedViewId to its NodeToViewDefinition --> make available through DDL IR
    val nodeToViewDefinitions = (for {
      nodeMapping <- graphDefinition.nodeMappings
      nodeToViewDefinition <- nodeMapping.nodeToViewDefinitions
    } yield AssignedViewIdentifier(nodeMapping.labelNames, nodeToViewDefinition.viewName) -> nodeToViewDefinition).toMap

    val nodeTables = nodeDataFramesWithIds.map {
      case (viewId, df) =>
        val nodeMapping = computeNodeMapping(viewId.labelDefinitions, graphSchema, nodeToViewDefinitions(viewId))
        CAPSNodeTable.fromMapping(nodeMapping, df)
    }.toSeq

    // Relationship tables

//    val relTypeToRelMapping = graphDefinition.relationshipMappings.map(rm => rm.relType -> rm).toMap
//    val relDataFramesWithoutIds = graphSchema.relationshipTypes.map { relType =>
//
//      val relationshipMappingDefinition = relTypeToRelMapping(relType)
//      val relTypeDfs = relationshipMappingDefinition.relationshipMappings.map { relationshipToViewDefinition =>
//
//        val sourceTable = relationshipToViewDefinition.sourceView.name
//        readSqlTable(sourceTable, sqlDataSourceConfig)
//
//      }
//      // TODO: keep all DFs, align column layout and union
//      // TODO: compute relationship mapping for each DF
//      relType -> relTypeDfs.head
//    }


    // RELTYPE { foo }
    // -> viewA -> start | end  | key1 + (key1 as foo)
    // -> viewB -> src   | trgt | key2 + (key2 as foo)


    // loading of just the rel DF
    // generate new id for rels
    // join with DF for start label combo -> add generated start node ids
    // join with DF for end label combo -> add generated end node ids
    //


    caps.graphs.create(nodeTables.head, nodeTables.tail: _*)
  }

  private def computeNodeMapping(
    labelCombination: Set[String],
    graphSchema: Schema,
    nodeToViewDefinition: NodeToViewDefinition
  ): NodeMapping = {
    val propertyToColumnMapping = nodeToViewDefinition.maybePropertyMapping match {
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

  private def readSqlTable(
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
