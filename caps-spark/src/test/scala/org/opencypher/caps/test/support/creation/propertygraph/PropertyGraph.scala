/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.test.support.creation.propertygraph

trait Graph {
  def nodes: Seq[Node]
  def relationships: Seq[Relationship]

  def getNodeById(id: Long): Option[Node] = {
    nodes.collectFirst {
      case n : Node if n.id == id => n
    }
  }

  def getRelationshipById(id: Long): Option[Relationship] = {
    relationships.collectFirst {
      case r : Relationship if r.id == id => r
    }
  }
}

case class PropertyGraph(nodes: Seq[Node], relationships: Seq[Relationship]) extends Graph {
  def updated(node: Node): PropertyGraph = copy(nodes = node +: nodes)

  def updated(rel: Relationship): PropertyGraph = copy(relationships = rel +: relationships)
}

object PropertyGraph {
  def empty: PropertyGraph = PropertyGraph(Seq.empty, Seq.empty)
}


trait GraphElement {
  def id: Long
  def properties: Map[String, Any]
}

case class Node(
  id: Long,
  labels: Set[String],
  properties: Map[String, Any]
) extends GraphElement

case class Relationship(
  id: Long,
  startId: Long,
  endId: Long,
  relType: String,
  properties: Map[String, Any]
) extends GraphElement


trait PropertyGraphFactory {
  def apply(createQuery: String, parameters: Map[String, Any]): PropertyGraph
}
