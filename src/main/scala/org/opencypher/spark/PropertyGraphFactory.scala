package org.opencypher.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.impl.{CachingPropertyGraphFactory, StdPropertyGraphFactory}

import scala.language.implicitConversions

object PropertyGraphFactory {
  def create(implicit session: SparkSession) =
    new CachingPropertyGraphFactory(new StdPropertyGraphFactory())
}

trait PropertyGraphFactory {

  final def add(data: EntityData) = data match {
    case nodeData: NodeData => addNode(nodeData)
    case relationshipData: RelationshipData => addRelationship(relationshipData)
  }

  final def addNode(data: NodeData): EntityId =
    addNode(data.labels.toSet, data.properties)

  final def addRelationship(data: RelationshipData): EntityId =
    addRelationship(data.startId, data.relationshipType, data.endId, data.properties)

  def addNode(labels: Set[String], properties: Map[String, CypherValue]): EntityId
  def addRelationship(startId: EntityId, relationshipType: String, endId: EntityId, properties: Map[String, CypherValue]): EntityId

  def clear(): Unit

  def graph: PropertyGraph
}



