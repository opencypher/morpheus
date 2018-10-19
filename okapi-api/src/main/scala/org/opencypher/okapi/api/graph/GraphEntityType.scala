package org.opencypher.okapi.api.graph

trait GraphEntityType {
  def name: String
}

case object Node extends GraphEntityType {
  override val name: String = "Node"
}

case object Relationship extends GraphEntityType {
  override val name: String = "Relationship"
}