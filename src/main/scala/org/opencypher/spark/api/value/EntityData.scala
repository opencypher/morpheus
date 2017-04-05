package org.opencypher.spark.api.value

object EntityData {

  object Creation extends Creation

  trait Creation {

    def newNode =
      NodeData.empty

    def newLabeledNode(labels: String*) =
      newNode.withLabels(labels: _*)

    def newUntypedRelationship(nodes: (CypherNode, CypherNode)): RelationshipData =
      newUntypedRelationship(nodes._1, nodes._2)

    def newUntypedRelationship(startNode: CypherNode, endNode: CypherNode): RelationshipData =
      RelationshipData(startNode.id, "", endNode.id)

    def newRelationship(triple: ((CypherNode, String), CypherNode)): RelationshipData =
      newRelationship(triple._1._1, triple._1._2, triple._2)

    def newRelationship(startNode: CypherNode, relType: String, endNode: CypherNode): RelationshipData =
      RelationshipData(startNode.id, relType, endNode.id)
  }
}

sealed trait EntityData {
  def asEntity(id: EntityId): CypherEntityValue
}

object NodeData {
  val empty = NodeData(labels = Array.empty, properties = Properties.empty)
}

final case class NodeData(labels: Array[String],
                          properties: Properties)
  extends EntityData {

  override def asEntity(id: EntityId) = CypherNode(id, labels, properties)

  def withLabels(newLabels: String*) = copy(labels = newLabels.toArray)
  def withProperties(newProperties: (String, CypherValue)*) = copy(properties = Properties(newProperties: _*))
  def withProperties(newProperties: Properties) = copy(properties = newProperties)
}

final case class RelationshipData(startId: EntityId,
                                  relationshipType: String,
                                  endId: EntityId,
                                  properties: Properties = Properties.empty)
  extends EntityData {

  override def asEntity(id: EntityId) = CypherRelationship(id, startId, endId, relationshipType, properties)

  def withStartId(newStartId: EntityId) = copy(startId = newStartId)
  def withRelationshipType(newType: String) = copy(relationshipType = newType)
  def withEndId(newEndId: EntityId) = copy(endId = newEndId)
  def withProperties(newProperties: (String, CypherValue)*) = copy(properties = Properties(newProperties: _*))
  def withProperties(newProperties: Properties) = copy(properties = newProperties)
}
