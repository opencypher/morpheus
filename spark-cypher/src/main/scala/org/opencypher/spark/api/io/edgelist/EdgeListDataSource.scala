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
package org.opencypher.spark.api.io.edgelist

import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.{PropertyKeys, PropertyGraphSchema}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.GraphElement.sourceIdKey
import org.opencypher.spark.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.spark.api.io.edgelist.EdgeListDataSource._
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.schema.CAPSSchema

object EdgeListDataSource {

  val NODE_LABEL = "V"

  val REL_TYPE = "E"

  val GRAPH_NAME = GraphName("graph")

  val SCHEMA: PropertyGraphSchema = CAPSSchema.empty
    .withNodePropertyKeys(Set(NODE_LABEL), PropertyKeys.empty)
    .withRelationshipPropertyKeys(REL_TYPE, PropertyKeys.empty)
}

/**
  * A read-only data source that is able to read graphs from edge list files. Input files are expected to contain one
  * edge per row, e.g.
  *
  * 0 1
  * 1 2
  *
  * describes a graph with two edges (one from vertex 0 to 1 and one from vertex 1 to 2).
  *
  * The data source can be parameterized with options used by the underlying Spark Csv reader.
  *
  * @param path    path to the edge list file
  * @param options Spark Csv reader options
  * @param caps    CAPS session
  */
case class EdgeListDataSource(path: String, options: Map[String, String] = Map.empty)(implicit caps: CAPSSession)
  extends PropertyGraphDataSource {

  override def hasGraph(name: GraphName): Boolean = name == GRAPH_NAME

  override def graph(name: GraphName): PropertyGraph = {
    val reader = options.foldLeft(caps.sparkSession.read) {
      case (current, (key, value)) => current.option(key, value)
    }

    val rawRels = reader
      .schema(StructType(Seq(
        StructField(sourceStartNodeKey, LongType),
        StructField(sourceEndNodeKey, LongType))))
      .csv(path)
      .withColumn(sourceIdKey, functions.monotonically_increasing_id())
      .select(sourceIdKey, sourceStartNodeKey, sourceEndNodeKey)

    val rawNodes = rawRels
      .select(rawRels.col(sourceStartNodeKey).as(sourceIdKey))
      .union(rawRels.select(rawRels.col(sourceEndNodeKey).as(sourceIdKey)))
      .distinct()

    caps.graphs.create(CAPSNodeTable(Set(NODE_LABEL), rawNodes), CAPSRelationshipTable(REL_TYPE, rawRels))
  }

  override def schema(name: GraphName): Option[PropertyGraphSchema] = Some(SCHEMA)

  override def store(name: GraphName, graph: PropertyGraph): Unit =
    throw UnsupportedOperationException("Storing an edge list is not supported")

  override def delete(name: GraphName): Unit =
    throw UnsupportedOperationException("Deleting an edge list is not supported")

  override val graphNames: Set[GraphName] = Set(GRAPH_NAME)
}
