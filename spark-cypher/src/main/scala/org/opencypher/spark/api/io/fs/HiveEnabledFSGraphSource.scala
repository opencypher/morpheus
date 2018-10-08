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
package org.opencypher.spark.api.io.fs

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.DefaultGraphDirectoryStructure.pathSeparator

/**
  * Additionally registers a Hive table in the Spark session.
  *
  * The naming scheme for Hive tables is:
  *
  * <pre>hiveDatabaseName.path_to_node_table_LabelA_LabelB</pre>
  *
  * and
  *
  * <pre>hiveDatabaseName.path_to_rel_table_RelKey</pre>
  *
  * respectively. The path is determined according to the default directory structure with path separators replaced by
  * underscores.
  *
  */
case class HiveEnabledFSGraphSource(
  hiveDatabaseName: String,
  override val rootPath: String,
  override val tableStorageFormat: String,
  override val filesPerTable: Option[Int] = Some(1)
)(implicit session: CAPSSession) extends FSGraphSource(rootPath, tableStorageFormat, filesPerTable) {

  private val sparkSession = session.sparkSession

  override def writeNodeTable(
    graphName: GraphName,
    labels: Set[String],
    table: DataFrame
  ): Unit = {
    super.writeNodeTable(graphName, labels, table)
    val pathToNodeTable = directoryStructure.pathToNodeTable(graphName, labels)
    val tableName = s"$hiveDatabaseName.${pathToNodeTable.replace(pathSeparator, "_")}"
    val options = Map("path" -> pathToNodeTable)
    sparkSession.catalog.createTable(tableName, tableStorageFormat, table.schema, options)
  }

  override def writeRelationshipTable(
    graphName: GraphName,
    relKey: String,
    table: DataFrame
  ): Unit = {
    super.writeRelationshipTable(graphName, relKey, table)
    val pathToRelTable = directoryStructure.pathToRelationshipTable(graphName, relKey)
    val tableName = s"$hiveDatabaseName.${pathToRelTable.replace(pathSeparator, "_")}"
    val options = Map("path" -> pathToRelTable)
    sparkSession.catalog.createTable(tableName, tableStorageFormat, table.schema, options)
  }

}
