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
package org.opencypher.caps.api.value

object EntityData {

  object Creation extends Creation

  trait Creation {

    def newNode: NodeData =
      NodeData.empty

    def newLabeledNode(labels: String*): NodeData =
      newNode.withLabels(labels: _*)

    def newUntypedRelationship(nodes: (CAPSNode, CAPSNode)): RelationshipData =
      newUntypedRelationship(nodes._1, nodes._2)

    def newUntypedRelationship(startNode: CAPSNode, endNode: CAPSNode): RelationshipData =
      RelationshipData(startNode.id, endNode.id, "")

    def newRelationship(triple: ((CAPSNode, String), CAPSNode)): RelationshipData =
      newRelationship(triple._1._1, triple._1._2, triple._2)

    def newRelationship(startNode: CAPSNode, relType: String, endNode: CAPSNode): RelationshipData =
      RelationshipData(startNode.id, endNode.id, relType)
  }
}

sealed trait EntityData {
  def asEntity(id: EntityId): CAPSEntityValue
}

object NodeData {
  val empty = NodeData(labels = Seq.empty, properties = Properties.empty)
}

final case class NodeData(labels: Seq[String],
                          properties: Properties)
  extends EntityData {

  override def asEntity(id: EntityId) = CAPSNode(id, labels, properties)

  def withLabels(newLabels: String*): NodeData = copy(labels = newLabels.toSeq)
  def withProperties(newProperties: (String, CAPSValue)*): NodeData = copy(properties = Properties(newProperties: _*))
  def withProperties(newProperties: Properties): NodeData = copy(properties = newProperties)
}

final case class RelationshipData(startId: EntityId,
                                  endId: EntityId,
                                  relationshipType: String,
                                  properties: Properties = Properties.empty)
  extends EntityData {

  override def asEntity(id: EntityId) = CAPSRelationship(id, startId, endId, relationshipType, properties)

  def withStartId(newStartId: EntityId): RelationshipData = copy(startId = newStartId)
  def withEndId(newEndId: EntityId): RelationshipData = copy(endId = newEndId)

  def withRelationshipType(newType: String): RelationshipData = copy(relationshipType = newType)

  def withProperties(newProperties: (String, CAPSValue)*): RelationshipData =
    copy(properties = Properties(newProperties: _*))

  def withProperties(newProperties: Properties): RelationshipData = copy(properties = newProperties)
}
