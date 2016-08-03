package org.opencypher.spark

trait PropertyGraphFactory {
  def addNode(properties: Map[String, CypherValue]): EntityId
  def addLabeledNode(labels: String*)(properties: Map[String, CypherValue] = Map.empty): EntityId
  def addRelationship(start: EntityId, end: EntityId, typ: String = "", properties: Map[String, CypherValue] = Map.empty): EntityId

  def reset(): Unit

  def result: PropertyGraph
}



