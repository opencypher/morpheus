/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.caps.impl.spark.io.neo4j

import org.apache.spark.rdd.RDD
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.opencypher.caps.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.caps.api.schema.Schema.NoLabel
import org.opencypher.caps.api.schema.{PropertyKeys, Schema, VerifiedSchema}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.convert.fromJavaType
import org.opencypher.caps.impl.spark.io.neo4j.external.{Neo4j, Neo4jConfig}

import scala.collection.JavaConverters._

object Neo4jGraphLoader {

  def loadSchema(config: Neo4jConfig, nodeQ: String, relQ: String)(implicit caps: CAPSSession): VerifiedSchema = {
    val (nodes, rels) = loadRDDs(config, nodeQ, relQ)

    loadSchema(nodes, rels)
  }

  private def loadSchema(nodes: RDD[InternalNode], rels: RDD[InternalRelationship]): VerifiedSchema = {

    def computeNodeSchema(schema: Schema, node: InternalNode): Schema = {
      val labels =
        if (node.labels().isEmpty) // could remove this unless we rely on `NoLabel`
          NoLabel
        else
          node.labels().asScala.toSet

      schema.withNodePropertyKeys(labels, convertProperties(node.asMap()))
    }

    def computeRelSchema(schema: Schema, relationship: InternalRelationship): Schema =
      schema.withRelationshipPropertyKeys(relationship.`type`(), convertProperties(relationship.asMap()))

    val nodeSchema = nodes.aggregate(Schema.empty)(computeNodeSchema, _ ++ _)
    val completeSchema = rels.aggregate(nodeSchema)(computeRelSchema, _ ++ _)

    completeSchema.verify
  }

  private def convertProperties(properties: java.util.Map[String, AnyRef]): PropertyKeys = {
    if (properties.isEmpty) {
      PropertyKeys.empty
    } else {
      properties.asScala.map {
        case (k, v) => k -> fromJavaType(v)
      }.toMap
    }
  }

  def fromNeo4j(config: Neo4jConfig)(implicit caps: CAPSSession): CAPSGraph =
    fromNeo4j(config, "MATCH (n) RETURN n", "MATCH ()-[r]->() RETURN r")

  def fromNeo4j(config: Neo4jConfig, nodeQuery: String, relQuery: String)(implicit caps: CAPSSession): CAPSGraph =
    fromNeo4j(config, nodeQuery, relQuery, "source", "rel", "target", None)

  def fromNeo4j(config: Neo4jConfig, nodeQuery: String, relQuery: String, schema: VerifiedSchema)(
      implicit caps: CAPSSession): CAPSGraph =
    fromNeo4j(config, nodeQuery, relQuery, "source", "rel", "target", Some(schema))

  def fromNeo4j(
      config: Neo4jConfig,
      nodeQuery: String,
      relQuery: String,
      sourceNode: String,
      rel: String,
      targetNode: String,
      maybeSchema: Option[VerifiedSchema] = None)(implicit caps: CAPSSession): CAPSGraph = {
    val (nodes, rels) = loadRDDs(config, nodeQuery, relQuery)

    val verified = maybeSchema.getOrElse(loadSchema(nodes, rels))

    new Neo4jGraph(verified.schema, caps)(nodes, rels, sourceNode, rel, targetNode)
  }

  private def loadRDDs(config: Neo4jConfig, nodeQ: String, relQ: String)(implicit caps: CAPSSession) = {
    val sparkSession = caps.sparkSession
    val neo4j = Neo4j(config, sparkSession)
    val nodes = neo4j.cypher(nodeQ).loadNodeRdds.map(row => row(0).asInstanceOf[InternalNode])
    val rels = neo4j.cypher(relQ).loadRowRdd.map(row => row(0).asInstanceOf[InternalRelationship])

    nodes -> rels
  }
}
