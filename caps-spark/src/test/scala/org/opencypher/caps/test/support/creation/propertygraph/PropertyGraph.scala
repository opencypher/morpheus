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
