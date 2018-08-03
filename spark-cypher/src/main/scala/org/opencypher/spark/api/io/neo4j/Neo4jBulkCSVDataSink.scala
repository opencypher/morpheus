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
package org.opencypher.spark.api.io.neo4j

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.DefaultGraphDirectoryStructure._
import org.opencypher.spark.api.io.fs.FSGraphSource
import org.opencypher.spark.api.io.neo4j.Neo4jBulkCSVDataSink._
import org.opencypher.spark.api.io.{GraphEntity, Relationship}
import org.opencypher.spark.schema.CAPSSchema

object Neo4jBulkCSVDataSink {

  private val SCRIPT_NAME = "import.sh"

  private val SCRIPT_TEMPLATE: String =
    """
      |#!/bin/sh
      |if [ $# -ne 1 ]
      |then
      |  echo "Please provide the path to your Neo4j installation (e.g. /usr/share/neo4j/)"
      |else
      |  ${1}bin/neo4j-admin import \
      |  --database=%s \
      |  --delimiter="," \
      |  --id-type=INTEGER \
      |%s \
      |%s
      |fi
      |""".stripMargin

  private val SCHEME_REGEX = "(^\\w+:|^)\\/\\/"

  implicit class DataTypeOps(val dt: DataType) extends AnyVal {
    def toNeo4jBulkImportType: String = {
      dt match {
        case StringType => "string"
        case LongType => "int"
        case BooleanType => "boolean"
        case DoubleType => "double"
        case other => throw IllegalArgumentException("supported Neo4j bulk import type", other)
      }
    }
  }
}

class Neo4jBulkCSVDataSink(override val rootPath: String)(implicit session: CAPSSession)
  extends FSGraphSource(rootPath, "csv", filesPerTable = Some(1)) {

  override protected def writeSchema(
    graphName: GraphName,
    schema: CAPSSchema
  ): Unit = {
    val nodeArguments = schema.labelCombinations.combos.toSeq.map { labels =>
      s"""--nodes:${labels.mkString(":")} "${schemaFileForNodes(graphName, labels)},${dataFileForNodes(graphName, labels)}""""
    }
    val relArguments = schema.relationshipTypes.toSeq.map { relType =>
      s"""--relationships:$relType "${schemaFileForRelationships(graphName, relType)},${dataFileForRelationships(graphName, relType)}""""
    }

    val importScript = String.format(
      SCRIPT_TEMPLATE,
      graphName.value,
      nodeArguments.mkString("  ", " \\\n  ", ""),
      relArguments.mkString("  ", " \\\n  ", ""))

    fileSystem.writeFile(directoryStructure.pathToGraphDirectory(graphName) / SCRIPT_NAME, importScript)
  }

  def schemaFileForNodes(
    graphName: GraphName,
    labels: Set[String]
  ): String = directoryStructure.pathToNodeTable(graphName, labels).replaceFirst(SCHEME_REGEX, "") / "schema.csv"
  def dataFileForNodes(
    graphName: GraphName,
    labels: Set[String]
  ): String = directoryStructure.pathToNodeTable(graphName, labels).replaceFirst(SCHEME_REGEX, "") / "par*.csv"

  def schemaFileForRelationships(
    graphName: GraphName,
    relType: String
  ): String = directoryStructure.pathToRelationshipTable(graphName, relType).replaceFirst(SCHEME_REGEX, "") / "pars.csv"

  def dataFileForRelationships(
    graphName: GraphName,
    relType: String
  ): String = directoryStructure.pathToRelationshipTable(graphName, relType).replaceFirst(SCHEME_REGEX, "") / "par*.csv"

  override protected def writeNodeTable(
    graphName: GraphName,
    labels: Set[String],
    table: DataFrame
  ): Unit = {

    val neoSchema = table.schema.fields.map {
      case field if field.name == GraphEntity.sourceIdKey => s"${labels.map(_.toLowerCase).mkString("_")}:ID"
      case field if field.name.isPropertyColumnName => s"${field.name.toProperty}:${field.dataType.toNeo4jBulkImportType}"
    }.mkString(",")

    super.writeNodeTable(graphName, labels, table)
    fileSystem.writeFile(schemaFileForNodes(graphName, labels), neoSchema)
  }

  override protected def writeRelationshipTable(
    graphName: GraphName,
    relKey: String,
    table: DataFrame
  ): Unit = {

    val tableWithoutId = table.drop(table.schema.fieldNames.find(_ == GraphEntity.sourceIdKey).get)

    val neoSchema = tableWithoutId.schema.fields.map {
      case field if field.name == Relationship.sourceStartNodeKey => ":START_ID"
      case field if field.name == Relationship.sourceEndNodeKey => ":END_ID"
      case field if field.name.isPropertyColumnName => s"${field.name.toProperty}:${field.dataType.toNeo4jBulkImportType}"
    }.mkString(",")

    super.writeRelationshipTable(graphName, relKey, tableWithoutId)
    fileSystem.writeFile(schemaFileForRelationships(graphName, relKey), neoSchema)
  }

  override def hasGraph(graphName: GraphName): Boolean = false

  override def graphNames: Set[GraphName] = throw UnsupportedOperationException("Write-only PGDS")
  override def delete(graphName: GraphName): Unit = throw UnsupportedOperationException("Write-only PGDS")
  override def graph(name: GraphName): PropertyGraph = throw UnsupportedOperationException("Write-only PGDS")
  override def schema(graphName: GraphName): Option[CAPSSchema] = throw UnsupportedOperationException("Write-only PGDS")
}
