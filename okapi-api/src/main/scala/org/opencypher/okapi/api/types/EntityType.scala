package org.opencypher.okapi.api.types

trait EntityType {
  def name: String
}

case object Node extends EntityType {
  override val name: String = "Node"
}

case object Relationship extends EntityType {
  override val name: String = "Relationship"
}