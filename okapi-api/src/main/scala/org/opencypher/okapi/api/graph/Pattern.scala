package org.opencypher.okapi.api.graph

import org.opencypher.okapi.api.graph.Pattern._
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}

sealed trait Direction
case object Outgoing extends Direction
case object Incomming extends Direction
case object Both extends Direction

case class Connection(
  source: Option[Entity],
  target: Option[Entity],
  direction: Direction
)

case class Entity(name: String, typ: CypherType)

object Pattern {
  val DEFAULT_NODE_NAME = "node"
  val DEFAULT_REL_NAME = "rel"

  implicit case object PatternOrdering extends Ordering[Pattern] {
    override def compare(
      x: Pattern,
      y: Pattern
    ): Int = x.topology.size.compare(y.topology.size)
  }
}

sealed trait Pattern {
  def entities: Set[Entity]
  def topology: Map[Entity, Connection]
}

case class NodePattern(nodeType: CTNode) extends Pattern {
  val nodeEntity = Entity(DEFAULT_NODE_NAME, nodeType)

  override def entities: Set[Entity] = Set( nodeEntity )
  override def topology: Map[Entity, Connection] = Map.empty
}

case class RelationshipPattern(relType: CTRelationship) extends Pattern {
  val relEntity = Entity(DEFAULT_REL_NAME, relType)

  override def entities: Set[Entity] = Set( relEntity )
  override def topology: Map[Entity, Connection] = Map.empty
}

case class NodeRelPattern(nodeType: CTNode, relType: CTRelationship) extends Pattern {

  val nodeEntity = Entity(DEFAULT_NODE_NAME, nodeType)
  val relEntity = Entity(DEFAULT_REL_NAME, relType)

  override def entities: Set[Entity] = {
    Set(
      nodeEntity,
      relEntity
    )
  }

  override def topology: Map[Entity, Connection] = Map(
    relEntity -> Connection(Some(nodeEntity), None, Outgoing)
  )
}

case class TripletPattern(sourceNodeType: CTNode, relType: CTRelationship, targetNodeType: CTNode) extends Pattern {
  val sourceEntity = Entity("source_" + DEFAULT_NODE_NAME, sourceNodeType)
  val targetEntity = Entity("target_" + DEFAULT_NODE_NAME, targetNodeType)
  val relEntity = Entity(DEFAULT_REL_NAME, relType)

  override def entities: Set[Entity] = Set(
    sourceEntity,
    relEntity,
    targetEntity
  )

  override def topology: Map[Entity, Connection] = Map(
    relEntity -> Connection(Some(sourceEntity), Some(targetEntity), Outgoing)
  )
}
