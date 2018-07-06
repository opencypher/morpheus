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
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap, Schema}
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.api.value.CypherValue.CypherList
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.impl.schema.SchemaImpl
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.AbstractPropertyGraphDataSource
import org.opencypher.spark.api.io.GraphEntity.sourceIdKey
import org.opencypher.spark.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.spark.api.io.metadata.CAPSGraphMetaData
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource.{defaultEntireGraphName, metaPrefix, metaPropertyKey}
import org.opencypher.spark.api.value.{CAPSNode, CAPSRelationship}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.encoders._
import org.opencypher.spark.impl.io.neo4j.Neo4jHelpers._
import org.opencypher.spark.impl.io.neo4j.external.Neo4j
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

object Neo4jPropertyGraphDataSource {

  val defaultEntireGraphName = GraphName("graph")

  val metaPrefix: String = "___"

  val metaPropertyKey: String = s"${metaPrefix}morpheusID"

}

case class Neo4jPropertyGraphDataSource(
  config: Neo4jConfig,
  omitImportFailures: Boolean = false,
  entireGraphName: GraphName = defaultEntireGraphName
)(implicit session: CAPSSession)
  extends AbstractPropertyGraphDataSource {

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

  override def tableStorageFormat: String = "neo4j"

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
    val graphSchema = SchemaFromProcedure(config, omitImportFailures) match {
      case None =>
        // TODO: add link to procedure installation
        throw UnsupportedOperationException("Neo4j PGDS requires okapi-neo4j-procedures to be installed in Neo4j")

      case Some(schema) => schema
    }
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

  override protected def deleteGraph(graphName: GraphName): Unit = {
    graphName.metaLabel match {
      case Some(metaLabel) =>
        config.cypher(
          s"""|MATCH (n:$metaLabel)
              |DETACH DELETE n
        """.stripMargin)

      case None => throw UnsupportedOperationException("Deleting the entire Neo4j graph is not supported")
    }
  }

  // Query construction for reading

  override def store(graphName: GraphName, graph: PropertyGraph): Unit = {
    checkStorable(graphName)

    val executorCount = session.sparkSession.sparkContext.statusTracker.getExecutorInfos.length

    val metaLabel = graphName.metaLabel match {
      case Some(meta) => meta
      case None => throw UnsupportedOperationException("Writing to the global Neo4j graph is not supported")
    }

    graph.schema.labelCombinations.combos.foreach { combo =>
      graph.asCaps.nodesWithExactLabels("n", combo)
        .asCaps
        .toCypherMaps
        .coalesce(executorCount)
        .map(map => map("n").cast[CAPSNode])
        .foreachPartition(writeNodes(config, metaLabel))
    }

    config.execute { session =>
      session.run(s"CREATE CONSTRAINT ON (n:$metaLabel) ASSERT n.$metaPropertyKey IS UNIQUE").consume()
    }

    graph.schema.relationshipTypes.foreach { relType =>
      graph.relationships("r", CTRelationship(relType))
        .asCaps
        .toCypherMaps
        .coalesce(executorCount)
        .map(map => map("r").cast[CAPSRelationship])
        .foreachPartition(writeRels(config, metaLabel))
    }

    graphNameCache += graphName
  }

  private[neo4j] def flatNodeQuery(graphName: GraphName, labels: Set[String], schema: Schema): String = {
    val nodeVar = "n"
    val props = schema.nodeKeys(labels).keys.toList.sorted match {
      case Nil => ""
      case nonempty => nonempty.mkString(s", $nodeVar.", s", $nodeVar.", "")
    }
    val labelCount = labels.size + graphName.metaLabel.size
    s"""|MATCH ($nodeVar:${(labels ++ graphName.metaLabel).mkString(":")})
        |WHERE LENGTH(LABELS($nodeVar)) = $labelCount
        |RETURN id($nodeVar) AS $sourceIdKey$props""".stripMargin
  }

  private[neo4j] def flatRelQuery(graphName: GraphName, relType: String, schema: Schema): String = {
    val metaLabelPredicate = graphName.metaLabel.map(":" + _).getOrElse("")

    val relVar = "r"
    val props = schema.relationshipKeys(relType).keys.toList.sorted match {
      case Nil => ""
      case nonempty => nonempty.mkString(s", $relVar.", s", $relVar.", "")
    }
    s"""|MATCH (s$metaLabelPredicate)-[$relVar:$relType]->(e$metaLabelPredicate)
        |RETURN id($relVar) AS $sourceIdKey, id(s) AS $sourceStartNodeKey, id(e) AS $sourceEndNodeKey$props""".stripMargin
  }

  // Not used by that PGDS
  // TODO: rename AbstractPGDS to RelationalPGDS and have an abstract version for non-relational

  override protected def writeSchema(graphName: GraphName, schema: CAPSSchema): Unit = ()

  override protected def writeCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: CAPSGraphMetaData): Unit = ()

  override protected def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit = ()

  override protected def writeRelationshipTable(graphName: GraphName, relKey: String, table: DataFrame): Unit = ()
}

case class writeNodes(config: Neo4jConfig, graphNameLabel: String) extends (Iterator[CAPSNode] => Unit) {
  override def apply(nodes: Iterator[CAPSNode]): Unit = {
    config.execute { session =>
      nodes.foreach { node =>
        val labels = node.labels
        val id = node.id

        val nodeLabels = if (labels.isEmpty) ""
        else labels.mkString(":`", "`:`", "`")

        val props = if (node.properties.isEmpty) ""
        else node.properties.toCypherString

        val createQ =
          s"""
             |CREATE (n:$graphNameLabel$nodeLabels $props)
             |SET n.$metaPropertyKey = $id
         """.stripMargin
        session.run(createQ).consume()
      }
    }
  }
}

case class writeRels(config: Neo4jConfig, graphNameLabel: String) extends (Iterator[CAPSRelationship] => Unit) {
  override def apply(rels: Iterator[CAPSRelationship]): Unit = {
    config.execute { session =>
      rels.foreach { relationship =>
        val id = relationship.id
        val startId = relationship.startId
        val endId = relationship.endId

        val props = if (relationship.properties.isEmpty) ""
        else relationship.properties.toCypherString

        val createQ =
          s"""
             |MATCH (a:$graphNameLabel), (b:$graphNameLabel)
             |WHERE a.$metaPropertyKey = $startId AND b.$metaPropertyKey = $endId
             |CREATE (a)-[r:${relationship.relType} $props]->(b)
             |SET r.$metaPropertyKey = $id
         """.stripMargin
        session.run(createQ).consume()
      }
    }
  }
}
