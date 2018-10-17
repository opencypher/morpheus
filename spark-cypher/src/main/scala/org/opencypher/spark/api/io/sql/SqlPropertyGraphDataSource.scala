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
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{AbstractPropertyGraphDataSource, JdbcFormat, StorageFormat}
import org.opencypher.spark.api.io.metadata.CAPSGraphMetaData
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.sql.ddl.DdlDefinitions

case class SqlPropertyGraphDataSource(
  ddl: DdlDefinitions,
  sqlDataSources: Map[String, SqlDataSourceConfig]
)(
  override implicit val caps: CAPSSession
) extends AbstractPropertyGraphDataSource {

  override def tableStorageFormat: StorageFormat = JdbcFormat

  override protected def listGraphNames: List[String] = ddl.graphSchemas.keySet.toList

  override protected def deleteGraph(graphName: GraphName): Unit =
    unsupported("deleting of source data")

  override protected def readSchema(graphName: GraphName): CAPSSchema =
    CAPSSchema(
      ddl.graphSchemas.getOrElse(graphName.value, notFound(graphName.value, ddl.graphSchemas.keySet))
    )

  override def store(graphName: GraphName, graph: PropertyGraph): Unit = unsupported("storing a graph")

  override protected def writeSchema(graphName: GraphName, schema: CAPSSchema): Unit = ???

  override protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData = ???

  override protected def writeCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: CAPSGraphMetaData): Unit = ???

  override protected def readNodeTable(graphName: GraphName, labels: Set[String], sparkSchema: StructType): DataFrame = ???

  override protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit = ???

  override protected def readRelationshipTable(graphName: GraphName, relKey: String, sparkSchema: StructType): DataFrame = ???

  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit = ???


  private val className = getClass.getSimpleName

  private def unsupported(operation: String): Nothing =
    throw UnsupportedOperationException(s"$className does not allow $operation")

  private def notFound(needle: Any, haystack: Traversable[Any]): Nothing =
    throw IllegalArgumentException(
      expected = s"one of ${stringList(haystack)}",
      actual = needle
    )

  private def stringList(elems: Traversable[Any]): String =
    elems.mkString("[", ",", "]")


}
