package org.opencypher.spark

import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.{EntityId, PropertyGraph}
import org.opencypher.spark.impl.newvalue._
import org.opencypher.spark.impl.{CachingPropertyGraphFactory, StdPropertyGraphFactory}

import scala.language.implicitConversions

object PropertyGraphFactory {
  def create(implicit session: SparkSession) =
    new CachingPropertyGraphFactory(new StdPropertyGraphFactory())
}

trait PropertyGraphFactory {

  self =>

  type Graph <: PropertyGraph

  final def apply(f: PropertyGraphFactory => Unit): PropertyGraphFactory = {
    f(self)
    self
  }

  final def add(data: NodeData): CypherNode = addNode(data)

  final def add(data: RelationshipData): CypherRelationship = addRelationship(data)

  final def addNode(data: NodeData): CypherNode =
    addNode(data.labels.toSet, data.properties)

  final def addRelationship(data: RelationshipData): CypherRelationship =
    addRelationship(data.startId, data.relationshipType, data.endId, data.properties)

  def addNode(labels: Set[String], properties: Map[String, CypherValue]): CypherNode
  def addRelationship(startId: EntityId, relationshipType: String, endId: EntityId, properties: Map[String, CypherValue]): CypherRelationship

  def clear(): Unit

  def graph: Graph
}



