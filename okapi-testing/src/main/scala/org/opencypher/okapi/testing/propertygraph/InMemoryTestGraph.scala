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
package org.opencypher.okapi.testing.propertygraph

import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNode, CypherRelationship}

trait InMemoryGraph {
  def nodes: Seq[InMemoryTestNode]
  def relationships: Seq[InMemoryTestRelationship]

  def getNodeById(id: Long): Option[InMemoryTestNode] = {
    nodes.collectFirst {
      case n : InMemoryTestNode if n.id == id => n
    }
  }

  def getRelationshipById(id: Long): Option[InMemoryTestRelationship] = {
    relationships.collectFirst {
      case r : InMemoryTestRelationship if r.id == id => r
    }
  }
}

case class InMemoryTestGraph(nodes: Seq[InMemoryTestNode], relationships: Seq[InMemoryTestRelationship]) extends InMemoryGraph {
  def updated(node: InMemoryTestNode): InMemoryTestGraph = copy(nodes = node +: nodes)

  def updated(rel: InMemoryTestRelationship): InMemoryTestGraph = copy(relationships = rel +: relationships)
}

object InMemoryTestGraph {
  def empty: InMemoryTestGraph = InMemoryTestGraph(Seq.empty, Seq.empty)
}

case class InMemoryTestNode(
  override val id: Long,
  override val labels: Set[String],
  override val properties: CypherMap
) extends CypherNode[Long] {

  type I = InMemoryTestNode

  override def copy(id: Long = id, labels: Set[String] = labels, properties: CypherMap = properties): InMemoryTestNode.this.type = {
    InMemoryTestNode(id, labels, properties).asInstanceOf[this.type]
  }
}

case class InMemoryTestRelationship(
  override val id: Long,
  override val startId: Long,
  override val endId: Long,
  override val relType: String,
  override val properties: CypherMap
) extends CypherRelationship[Long] {

  type I = InMemoryTestRelationship

  override def copy(id: Long = id, source: Long = startId, target: Long = endId, relType: String = relType, properties: CypherMap = properties): InMemoryTestRelationship.this.type = {
    InMemoryTestRelationship(id, source, target, relType, properties).asInstanceOf[this.type]
  }

}

trait InMemoryGraphFactory {
  def apply(createQuery: String, parameters: Map[String, Any]): InMemoryTestGraph
}
