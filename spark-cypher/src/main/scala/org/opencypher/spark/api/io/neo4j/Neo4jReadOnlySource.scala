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
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.impl.schema.SchemaImpl
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.ROAbstractGraphSource
import org.opencypher.spark.api.io.metadata.CAPSGraphMetaData
import org.opencypher.spark.api.io.neo4j.Neo4jReadOnlySource.metaPrefix
import org.opencypher.spark.impl.io.neo4j.external.Neo4j
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

/**
  * A data source implementation that enables loading property graphs from a Neo4j database. A graph is identified by a
  * [[GraphName]] and parameterized by a node and a relationship query which are used to load the graph from Neo4j.
  *
  * If the [[Schema]] of a Neo4j graph is known upfront, it can be provided to the data source. Otherwise, the schema
  * will be computed during graph loading.
  *
  * @param config Neo4j connection configuration
  */
case class Neo4jReadOnlySource(
  config: Neo4jConfig,
  entireGraph: GraphName = GraphName("graph")
)(implicit session: CAPSSession)
  extends ROAbstractGraphSource {

  override def tableStorageFormat: String = "neo4j"

  override protected def listGraphNames: List[String] = List(entireGraph.value) // TODO: extend with meta label approach

  private def getMetaLabel(graphName: GraphName): Option[String] = graphName match {
    case `entireGraph` => None
    case subGraph => Some(metaPrefix + subGraph)
  }

  override protected def readSchema(graphName: GraphName): CAPSSchema = {
    val graphSchema = SchemaFromProcedure(config) match {
      case None => throw UnsupportedOperationException("needs the procedure")
      case Some(schema) => schema
    }
    val filteredSchema = getMetaLabel(graphName) match {
      case None =>
        graphSchema

      case Some(metaLabel) =>
        val labelPropertyMap = graphSchema.labelPropertyMap.filterForLabels(metaLabel)
        SchemaImpl(labelPropertyMap, graphSchema.relTypePropertyMap)
    }

    filteredSchema.asCaps
  }

  override protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData = CAPSGraphMetaData(tableStorageFormat)

  override protected def readNodeTable(
    graphName: GraphName,
    labels: Set[String],
    sparkSchema: StructType
  ): DataFrame = {
    val graphSchema = schema(graphName).get
    val flatQuery = flatNodeQuery(graphName, labels, graphSchema)

    val neo4jConnection = Neo4j(config, session.sparkSession)
    val rdd = neo4jConnection.cypher(flatQuery).loadRowRdd
    session.sparkSession.createDataFrame(rdd, sparkSchema)
  }

  override protected def readRelationshipTable(
    graphName: GraphName,
    relKey: String,
    sparkSchema: StructType
  ): DataFrame = {
    val graphSchema = schema(graphName).get
    val flatQuery = flatRelQuery(graphName, relKey, graphSchema)

    val neo4jConnection = Neo4j(config, session.sparkSession)
    val rdd = neo4jConnection.cypher(flatQuery).loadRowRdd
    session.sparkSession.createDataFrame(rdd, sparkSchema)
  }

  def flatNodeQuery(graphName: GraphName, labels: Set[String], schema: Schema): String = {
    val metaLabel = getMetaLabel(graphName)

    val nodeVar = "n"
    val props = schema.nodeKeys(labels).keys.toList.sorted match {
      case Nil => ""
      case nonempty => nonempty.mkString(s", $nodeVar.", s", $nodeVar.", "")
    }
    s"MATCH ($nodeVar:${(labels ++ metaLabel).mkString(":")}) RETURN id($nodeVar) AS id$props"
  }

  def flatRelQuery(graphName: GraphName, relType: String, schema: Schema): String = {
    val metaLabelPredicate = getMetaLabel(graphName).map(":" + _).getOrElse("")

    val relVar = "r"
    val props = schema.relationshipKeys(relType).keys.toList.sorted match {
      case Nil => ""
      case nonempty => nonempty.mkString(s", $relVar.", s", $relVar.", "")
    }
    s"MATCH ($metaLabelPredicate)-[$relVar:$relType]->($metaLabelPredicate) RETURN id($relVar) AS id$props"
  }
}

object Neo4jReadOnlySource {
  val metaPrefix: String = "___"
}
