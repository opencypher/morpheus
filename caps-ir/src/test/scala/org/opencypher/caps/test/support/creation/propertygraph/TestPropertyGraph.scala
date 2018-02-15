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

import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherNode, CypherRelationship}

trait Graph {
  def nodes: Seq[TestNode]
  def relationships: Seq[TestRelationship]

  def getNodeById(id: Long): Option[TestNode] = {
    nodes.collectFirst {
      case n : TestNode if n.id == id => n
    }
  }

  def getRelationshipById(id: Long): Option[TestRelationship] = {
    relationships.collectFirst {
      case r : TestRelationship if r.id == id => r
    }
  }
}

case class TestPropertyGraph(nodes: Seq[TestNode], relationships: Seq[TestRelationship]) extends Graph {
  def updated(node: TestNode): TestPropertyGraph = copy(nodes = node +: nodes)

  def updated(rel: TestRelationship): TestPropertyGraph = copy(relationships = rel +: relationships)
}

object TestPropertyGraph {
  def empty: TestPropertyGraph = TestPropertyGraph(Seq.empty, Seq.empty)
}

case class TestNode(
  override val id: Long,
  override val labels: Set[String],
  override val properties: CypherMap
) extends CypherNode[Long] {

  type I = TestNode

  override def copy(id: Long = id, labels: Set[String] = labels, properties: CypherMap = properties) = {
    TestNode(id, labels, properties).asInstanceOf[this.type]
  }
}

case class TestRelationship(
  override val id: Long,
  override val source: Long,
  override val target: Long,
  override val relType: String,
  override val properties: CypherMap
) extends CypherRelationship[Long] {

  type I = TestRelationship

  override def copy(id: Long = id, source: Long = source, target: Long = target, relType: String = relType, properties: CypherMap = properties) = {
    TestRelationship(id, source, target, relType, properties).asInstanceOf[this.type]
  }

}


trait PropertyGraphFactory {
  def apply(createQuery: String, parameters: Map[String, Any]): TestPropertyGraph
}
