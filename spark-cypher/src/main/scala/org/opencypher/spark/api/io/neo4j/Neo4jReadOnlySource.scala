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
import org.neo4j.driver.v1.Value
import org.neo4j.driver.v1.util.Function
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.impl.schema.SchemaImpl
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.AbstractDataSource
import org.opencypher.spark.api.io.metadata.CAPSGraphMetaData
import org.opencypher.spark.api.io.neo4j.Neo4jReadOnlySource.{metaPrefix, schemaProcedureName}
import org.opencypher.spark.impl.io.neo4j.Neo4jHelpers._
import org.opencypher.spark.impl.io.neo4j.external.Neo4j
import org.opencypher.spark.schema.CAPSSchema

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


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
  extends AbstractDataSource {

  private def computeSchema(name: GraphName): Option[Schema] = {
    Try {
      config.cypher("CALL dbms.procedures() YIELD name AS name").exists { map =>
        map("name").value == schemaProcedureName
      }
    } match {
      case Success(true) =>
        schemaFromProcedure(config)
      case Success(false) =>
        System.err.println("Neo4j schema procedure not activated. Consider activating the procedure `" + schemaProcedureName + "`.")
        None
      case Failure(error) =>
        System.err.println(s"Retrieving the procedure list from the Neo4j database failed: $error")
        None
    }
  }

  private[neo4j] def schemaFromProcedure(config: Neo4jConfig): Option[Schema] = {
    Try {
      val rows = config.execute { session =>
        val result = session.run("CALL " + schemaProcedureName)

        result.list().asScala.map { row =>
          val typ = row.get("type").asString()
          val labels = row.get("nodeLabelsOrRelType").asList(new Function[Value, String] {
            override def apply(v1: Value): String = v1.asString()
          }).asScala.toList

          row.get("property").asString() match {
            case "" => // this label/type has no properties
              (typ, labels, None, None)
            case property =>
              val typeString = row.get("cypherType").asString()
              val cypherType = CypherType.fromName(typeString)
              (typ, labels, Some(property), cypherType)
          }
        }
      }

      rows.groupBy(row => row._1 -> row._2).map {
        case ((typ, labels), tuples) =>
          val properties = tuples.collect {
            case (_, _, p, t) if p.nonEmpty => p.get -> t.get
          }

          typ match {
            case "Node" =>
              Schema.empty.withNodePropertyKeys(labels: _*)(properties: _*)
            case "Relationship" =>
              Schema.empty.withRelationshipPropertyKeys(labels.headOption.getOrElse(""))(properties: _*)
          }
      }.foldLeft(Schema.empty)(_ ++ _)
    } match {
      case Success(schema) => Some(schema)
      case Failure(error) =>
        System.err.println(s"Could not load schema from Neo4j: ${error.getMessage}")
        error.printStackTrace()
        None
    }
  }

  override def tableStorageFormat: String = "neo4j"

  override protected def listGraphNames: List[String] = List(entireGraph.value) // TODO: extend with meta label approach

  private def getMetaLabel(graphName: GraphName): Option[String] = graphName match {
    case `entireGraph` => None
    case subGraph => Some(metaPrefix + subGraph)
  }

  override protected def readSchema(graphName: GraphName): CAPSSchema = {
    val graphSchema = computeSchema(graphName).get
    val filteredSchema = getMetaLabel(graphName) match {
      case None =>
        graphSchema

      case Some(metaLabel) =>
        val labelPropertyMap = graphSchema.labelPropertyMap.filterForLabels(metaLabel)
        SchemaImpl(labelPropertyMap, graphSchema.relTypePropertyMap)
    }

    import org.opencypher.spark.schema.CAPSSchema._

    filteredSchema.asCaps
  }

  override protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData = CAPSGraphMetaData(tableStorageFormat)

  override protected def readNodeTable(
    graphName: GraphName,
    labels: Set[String],
    sparkSchema: StructType
  ): DataFrame = {
    val graphSchema = schema(graphName).get

    val neo4jConnection = Neo4j(config, session.sparkSession)

    val flatQuery = flatNodeQuery(graphName, labels, graphSchema)

    session.sparkSession.createDataFrame(neo4jConnection.cypher(flatQuery).loadRowRdd, sparkSchema)
  }

  def flatNodeQuery(graphName: GraphName, labels: Set[String], schema: Schema): String = {
    val metaLabel = getMetaLabel(graphName)

    val nodeVar = "n"
    val props = schema.nodeKeys(labels).keys.toList match {
      case Nil => ""
      case nonempty => nonempty.mkString(s", $nodeVar.", s", $nodeVar.", "")
    }
    s"MATCH ($nodeVar:${(labels ++ metaLabel).mkString(":")}) RETURN id($nodeVar) AS id$props"
  }

  def flatRelQuery(graphName: GraphName, relType: String, schema: Schema): String = {
    val metaLabelPredicate = getMetaLabel(graphName).map(":" + _).getOrElse("")

    val relVar = "r"
    val props = schema.relationshipKeys(relType).keys.toList match {
      case Nil => ""
      case nonempty => nonempty.mkString(s", $relVar.", s", $relVar.", "")
    }
    s"MATCH ($metaLabelPredicate)-[$relVar:$relType]->($metaLabelPredicate) RETURN id($relVar) AS id$props"
  }

  override protected def readRelationshipTable(
    graphName: GraphName,
    relKey: String,
    sparkSchema: StructType
  ): DataFrame = {
    val graphSchema = schema(graphName).get

    val flatQuery = flatRelQuery(graphName, relKey, graphSchema)

    ???
  }

  override protected def deleteGraph(graphName: GraphName): Unit = ???
  override protected def writeSchema(
    graphName: GraphName,
    schema: CAPSSchema
  ): Unit = ???
  override protected def writeCAPSGraphMetaData(
    graphName: GraphName,
    capsGraphMetaData: CAPSGraphMetaData
  ): Unit = ???
  override protected def writeNodeTable(
    graphName: GraphName,
    labels: Set[String],
    table: DataFrame
  ): Unit = ???
  override protected def writeRelationshipTable(
    graphName: GraphName,
    relKey: String,
    table: DataFrame
  ): Unit = ???
}

object Neo4jReadOnlySource {
  val metaPrefix: String = "___"
  val schemaProcedureName = "org.neo4j.morpheus.procedures.schema"
}
