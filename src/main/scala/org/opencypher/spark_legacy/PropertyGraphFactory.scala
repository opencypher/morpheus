/**
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
package org.opencypher.spark_legacy

import org.apache.spark.sql.SparkSession
import org.opencypher.spark_legacy.api.PropertyGraph
import org.opencypher.spark_legacy.impl.{CachingPropertyGraphFactory, StdPropertyGraphFactory}
import org.opencypher.spark.api.value._

import scala.language.implicitConversions

object PropertyGraphFactory {
  def create(implicit session: SparkSession) =
    new CachingPropertyGraphFactory(new StdPropertyGraphFactory())
}

trait PropertyGraphFactory {

  self =>

  type Graph <: PropertyGraph

  final def apply(f: PropertyGraphFactory => Unit): PropertyGraphFactory = {
    f(self)
    self
  }

  final def add(data: NodeData): CypherNode = addNode(data)

  final def add(data: RelationshipData): CypherRelationship = addRelationship(data)

  final def addNode(data: NodeData): CypherNode =
    addNode(data.labels.toSet, data.properties)

  final def addRelationship(data: RelationshipData): CypherRelationship =
    addRelationship(data.startId, data.relationshipType, data.endId, data.properties)

  def addNode(labels: Set[String], properties: Properties): CypherNode
  def addRelationship(startId: EntityId, relationshipType: String, endId: EntityId,
                      properties: Properties): CypherRelationship

  def clear(): Unit

  def graph: Graph
}



