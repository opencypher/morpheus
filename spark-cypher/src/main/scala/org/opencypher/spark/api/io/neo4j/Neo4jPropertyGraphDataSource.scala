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

import java.util.concurrent.Executors

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap}
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap, Schema}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherList
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.impl.schema.SchemaImpl
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.neo4j.io.{EntityReader, EntityWriter, Neo4jConfig}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.value.{CAPSNode, CAPSRelationship}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.encoders._
import org.opencypher.spark.impl.io.neo4j.external.Neo4j
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

case class Neo4jPropertyGraphDataSource(
  override val config: Neo4jConfig,
  maybeSchema: Option[Schema] = None,
  override val omitIncompatibleProperties: Boolean = false,
  entireGraphName: GraphName = defaultEntireGraphName
)(override implicit val session: CAPSSession) extends AbstractNeo4jDataSource with Logging {

  graphNameCache += entireGraphName

  private implicit class RichGraphName(graphName: GraphName) {
    def metaLabel: Option[String] = graphName match {
      case `entireGraphName` => None
      case subGraph => Some(metaPrefix + subGraph)
    }
  }

  private implicit class RichPropertyKeys(keys: PropertyKeys) {

    def withoutMetaProperty: PropertyKeys =
      keys.filterKeys(k => k != metaPropertyKey)
  }

  private implicit class RichLabelPropertyMap(map: LabelPropertyMap) {

    def withoutMetaLabel(metaLabel: String): LabelPropertyMap =
      LabelPropertyMap(map.map.map { case (k, v) => (k - metaLabel) -> v })

    def withoutMetaProperty: LabelPropertyMap =
      LabelPropertyMap(map.map.mapValues(_.withoutMetaProperty))
  }

  private implicit class RichRelTypePropertyMap(map: RelTypePropertyMap) {

    def withoutMetaProperty: RelTypePropertyMap =
      RelTypePropertyMap(map.map.mapValues(_.withoutMetaProperty))
  }

  override def hasGraph(graphName: GraphName): Boolean = graphName match {
    case `defaultEntireGraphName` => true
    case _ => super.hasGraph(graphName)
  }

  override protected def listGraphNames: List[String] = {
    val labelResult = config.cypher(
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

  override protected def readSchema(graphName: GraphName): CAPSSchema = {
    val graphSchema = maybeSchema.getOrElse(super.readSchema(graphSchema))

    val filteredSchema = graphName.metaLabel match {
      case None =>
        graphSchema
      case Some(metaLabel) =>
        val containsMetaLabel = graphSchema.labelPropertyMap.filterForLabels(metaLabel)
        val cleanLabelPropertyMap = containsMetaLabel.withoutMetaLabel(metaLabel).withoutMetaProperty
        val cleanRelTypePropertyMap = graphSchema.relTypePropertyMap.withoutMetaProperty
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
    val flatQuery = EntityReader.flatRelTypeQuery(relKey, graphSchema, graphName.metaLabel)

    val neo4jConnection = Neo4j(config, session.sparkSession)
    val rdd = neo4jConnection.cypher(flatQuery).loadRowRdd
    session.sparkSession.createDataFrame(rdd, sparkSchema)
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

    val executorCount = session.sparkSession.sparkContext.statusTracker.getExecutorInfos.length
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(executorCount))

    logger.debug(s"Using $executorCount Threads")

    val metaLabel = graphName.metaLabel match {
      case Some(meta) => meta
      case None => throw UnsupportedOperationException("Writing to the global Neo4j graph is not supported")
    }

    config.withSession { session =>
      session.run(s"CREATE CONSTRAINT ON (n:$metaLabel) ASSERT n.$metaPropertyKey IS UNIQUE").consume()
    }

    val writesCompleted = for {
      _ <- Future.sequence(NodeWriter(graph, metaLabel, config))
      _ <- Future.sequence(RelWriter(graph, metaLabel, config))
    } yield Future{ }
    Await.result(writesCompleted, Duration.Inf)

    graphNameCache += graphName
  }

  // No need to implement these as we overwrite {{{org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource.store}}}
  override protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit = ()
  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit = ()
}

case object NodeWriter {
  def apply(graph: PropertyGraph, metaLabel: String, config: Neo4jConfig)
    (implicit executionContext: ExecutionContextExecutorService): Seq[Future[Unit]] = {
    graph.schema.labelCombinations.combos.map { combo =>
        graph.asCaps.nodes("n", CTNode(combo), exactLabelMatch = true)
          .asCaps
          .toCypherMaps
          .map(map => map("n").cast[CAPSNode])
          .rdd.foreachPartitionAsync(i => EntityWriter.writeNodes(i, config, Some(metaLabel)))
    }
  }.toSeq
}

case object RelWriter {
  def apply(graph: PropertyGraph, metaLabel: String, config: Neo4jConfig)
    (implicit executionContext: ExecutionContextExecutorService): Seq[Future[Unit]] = {
    graph.schema.relationshipTypes.map { relType =>
      graph.relationships("r", CTRelationship(relType))
        .asCaps
        .toCypherMaps
        .map(map => map("r").cast[CAPSRelationship])
        .rdd.foreachPartitionAsync(i => EntityWriter.writeRelationships(i, config, Some(metaLabel)))
    }.toSeq
  }
}