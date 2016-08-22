package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.api.EntityId

object EntityData {

  object Creation extends Creation

  trait Creation {

    def newNode =
      NodeData.empty

    def newLabeledNode(labels: String*) =
      newNode.withLabels(labels: _*)

//    def newUntypedRelationship(nodes: (CypherNode, CypherNode)): RelationshipData =
//      newUntypedRelationship(nodes._1, nodes._2)
//
//    def newUntypedRelationship(startNode: CypherNode, endNode: CypherNode): RelationshipData =
//      RelationshipData(startNode.id, "", endNode.id)
//
//    def newRelationship(triple: ((CypherNode, String), CypherNode)): RelationshipData =
//      newRelationship(triple._1._1, triple._1._2, triple._2)
//
//    def newRelationship(startNode: CypherNode, relType: String, endNode: CypherNode): RelationshipData =
//      RelationshipData(startNode.id, relType, endNode.id)
  }
}

sealed trait EntityData {
  def asEntity(id: EntityId): CypherEntityValue
}

object NodeData {
  val empty = NodeData(labels = Seq.empty, properties = Map.empty)
}

final case class NodeData(labels: Seq[String],
                          properties: Map[String, CypherValue])
  extends EntityData {

  override def asEntity(id: EntityId) = CypherNode(id, labels, properties)

  def withLabels(newLabels: String*) = copy(labels = newLabels)

  def withProperties(newProperties: (String, CypherValue)*) = copy(properties = newProperties.toMap)

  def withProperties(newProperties: Map[String, CypherValue]) = copy(properties = newProperties)
}

//final case class RelationshipData(startId: EntityId,
//                                  relationshipType: String,
//                                  endId: EntityId,
//                                  properties: Map[String, CypherValue] = Map.empty)
//  extends EntityData {
//
//  override def asEntity(id: EntityId) = CypherRelationship(id, startId, relationshipType, endId, properties)
//
//  def withStartId(newStartId: EntityId) = copy(startId = newStartId)
//
//  def withRelationshipType(newType: String) = copy(relationshipType = newType)
//
//  def withEndId(newEndId: EntityId) = copy(endId = newEndId)
//
//  def withProperties(newProperties: (String, CypherValue)*) = copy(properties = newProperties.toMap)
//
//  def withProperties(newProperties: Map[String, CypherValue]) = copy(properties = newProperties)
//}
