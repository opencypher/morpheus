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
package org.opencypher.spark.api.io.neo4j

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.types.{BinaryType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.unsafe.types.CalendarInterval
import org.neo4j.driver.v1.{Value, Values}
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.LabelPropertyMap._
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherList
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.impl.schema.SchemaImpl
import org.opencypher.okapi.ir.api.expr.{EndNode, Property, StartNode}
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.neo4j.io.{EntityReader, EntityWriter, Neo4jConfig}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.encoders.LongEncoder._
import org.opencypher.spark.impl.io.neo4j.external.Neo4j
import org.opencypher.spark.impl.table.SparkTable._
import org.opencypher.spark.impl.temporal.SparkTemporalHelpers._
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

case class Neo4jPropertyGraphDataSource(
  override val config: Neo4jConfig,
  maybeSchema: Option[Schema] = None,
  override val omitIncompatibleProperties: Boolean = false
)(implicit val caps: CAPSSession) extends AbstractNeo4jDataSource with Logging {

  graphNameCache += entireGraphName

  override def hasGraph(graphName: GraphName): Boolean = graphName match {
    case `entireGraphName` => true
    case _ => super.hasGraph(graphName)
  }

  override protected def listGraphNames: List[String] = {
    val labelResult = config.cypherWithNewSession(
      """|CALL db.labels()
         |YIELD label
         |RETURN collect(label) AS labels
      """.stripMargin)
    val allLabels = labelResult.head("labels").cast[CypherList].value.map(_.toString)

    val metaLabelGraphNames = allLabels
      .filter(_.startsWith(metaPrefix))
      .map(_.drop(metaPrefix.length))
      .distinct

    metaLabelGraphNames
  }

  private lazy val entireGraphSchema: Schema = {
    maybeSchema.getOrElse(super.readSchema(entireGraphName))
  }

  override protected def readSchema(graphName: GraphName): CAPSSchema = {
    val filteredSchema = graphName.metaLabel match {
      case None =>
        entireGraphSchema
      case Some(metaLabel) =>
        val containsMetaLabel = entireGraphSchema.labelPropertyMap.filterForLabels(metaLabel)
        val cleanLabelPropertyMap = containsMetaLabel.withoutMetaLabel(metaLabel).withoutMetaProperty
        val cleanRelTypePropertyMap = entireGraphSchema.relTypePropertyMap.withoutMetaProperty
        SchemaImpl(cleanLabelPropertyMap, cleanRelTypePropertyMap)
    }
    filteredSchema.asCaps
  }

  override protected def readNodeTable(
    graphName: GraphName,
    labels: Set[String],
    sparkSchema: StructType
  ): DataFrame = {
    val graphSchema = schema(graphName).get
    val flatQuery = EntityReader.flatExactLabelQuery(labels, graphSchema, graphName.metaLabel)

    val neo4jConnection = Neo4j(config, caps.sparkSession)
    val rdd = neo4jConnection.cypher(flatQuery).loadRowRdd

    // encode Neo4j identifiers to BinaryType
    caps.sparkSession
      .createDataFrame(rdd, sparkSchema.convertTypes(BinaryType, LongType))
      .transformColumns(idPropertyKey)(_.encodeLongAsCAPSId)
  }

  override protected def readRelationshipTable(
    graphName: GraphName,
    relKey: String,
    sparkSchema: StructType
  ): DataFrame = {
    val graphSchema = schema(graphName).get
    val flatQuery = EntityReader.flatRelTypeQuery(relKey, graphSchema, graphName.metaLabel)

    val neo4jConnection = Neo4j(config, caps.sparkSession)
    val rdd = neo4jConnection.cypher(flatQuery).loadRowRdd

    // encode Neo4j identifiers to BinaryType
    caps.sparkSession
      .createDataFrame(rdd, sparkSchema.convertTypes(BinaryType, LongType))
      .transformColumns(idPropertyKey, startIdPropertyKey, endIdPropertyKey)(_.encodeLongAsCAPSId)
  }

  override protected def deleteGraph(graphName: GraphName): Unit = {
    graphName.metaLabel match {
      case Some(metaLabel) =>
        config.withSession { session =>
          session.run(
            s"""|MATCH (n:$metaLabel)
                |DETACH DELETE n
        """.stripMargin).consume()
        }
      case None => throw UnsupportedOperationException("Deleting the entire Neo4j graph is not supported")
    }
  }

  // Query construction for reading

  override def store(graphName: GraphName, graph: PropertyGraph): Unit = {
    checkStorable(graphName)

    val metaLabel = graphName.metaLabel match {
      case Some(meta) => meta
      case None => throw UnsupportedOperationException("Writing to the global Neo4j graph is not supported")
    }

    config.withSession { session =>
      logger.info(s"Creating database uniqueness constraint on $metaLabel.$metaPropertyKey")
      session.run(s"CREATE CONSTRAINT ON (n:$metaLabel) ASSERT n.$metaPropertyKey IS UNIQUE").consume()
    }

    val writesCompleted = for {
      _ <- Future.sequence(Writers.writeNodes(graph, metaLabel, config))
      _ <- Future.sequence(Writers.writeRelationships(graph, metaLabel, config))
    } yield Future {}
    Await.result(writesCompleted, Duration.Inf)

    schemaCache += graphName -> graph.schema.asCaps
    graphNameCache += graphName
  }

  // No need to implement these as we overwrite {{{org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource.store}}}
  override protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit = ()
  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit = ()
}

case object Writers {
  def writeNodes(graph: PropertyGraph, metaLabel: String, config: Neo4jConfig)
    (implicit caps: CAPSSession): Set[Future[Unit]] = {
    val result: Set[Future[Unit]] = graph.schema.labelCombinations.combos.map { combo =>
      val nodeScan = graph.nodes("n", CTNode(combo), exactLabelMatch = true).asCaps
      val mapping = computeMapping(nodeScan)
      nodeScan
        .df
        .encodeBinaryToHexString
        .rdd
        .foreachPartitionAsync { i =>
          if (i.nonEmpty) EntityWriter.createNodes(i, mapping, config, combo + metaLabel)(rowToListValue)
        }
    }
    result
  }

  def writeRelationships(graph: PropertyGraph, metaLabel: String, config: Neo4jConfig)
    (implicit caps: CAPSSession): Set[Future[Unit]] = {
    graph.schema.relationshipTypes.map { relType =>
      val relScan = graph.relationships("r", CTRelationship(relType)).asCaps
      val mapping = computeMapping(relScan)

      val header = relScan.header
      val relVar = header.entityVars.head
      val startExpr = header.expressionsFor(relVar).collect { case s: StartNode => s }.head
      val endExpr = header.expressionsFor(relVar).collect { case e: EndNode => e }.head
      val startColumn = relScan.header.column(startExpr)
      val endColumn = relScan.header.column(endExpr)
      val startIndex = relScan.df.columns.indexOf(startColumn)
      val endIndex = relScan.df.columns.indexOf(endColumn)

      relScan
        .df
        .encodeBinaryToHexString
        .rdd
        .foreachPartitionAsync { i =>
          if (i.nonEmpty) {
            EntityWriter.createRelationships(
              i,
              startIndex,
              endIndex,
              mapping,
              config,
              relType,
              Some(metaLabel)
            )(rowToListValue)
          }
        }
    }
  }

  private def rowToListValue(row: Row): Value = {
    val array = new Array[Value](row.size)
    var i = 0
    while (i < row.size) {
      val castedValue = row.get(i) match {
        case d: java.sql.Date => d.toLocalDate
        case ts: java.sql.Timestamp => ts.toLocalDateTime
        case ci: CalendarInterval => ci.toJavaDuration
        case other => other
      }
      array(i) = Values.value(castedValue)
      i += 1
    }
    Values.value(array: _*)
  }

  private def computeMapping(nodeScan: CAPSRecords): Array[String] = {
    val header = nodeScan.header
    val nodeVar = header.entityVars.head
    val properties: Set[Property] = header.expressionsFor(nodeVar).collect {
      case p: Property => p
    }

    val columns = nodeScan.df.columns
    val mapping = Array.fill[String](columns.length)(null)

    val idIndex = columns.indexOf(header.column(nodeVar))
    mapping(idIndex) = metaPropertyKey

    properties.foreach { property =>
      val index = columns.indexOf(header.column(property))
      mapping(index) = property.key.name
    }

    mapping
  }
}