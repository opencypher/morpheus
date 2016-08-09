package org.opencypher.spark.impl

import org.opencypher.spark.api.PropertyGraph
import org.opencypher.spark.{EntityId, CypherValue, PropertyGraphFactory}

class CachingPropertyGraphFactory(inner: PropertyGraphFactory) extends PropertyGraphFactory {
  private var graphCache: Option[PropertyGraph] = None

  def addNode(labels: Set[String], properties: Map[String, CypherValue]): EntityId =
    flushAfter { inner.addNode(labels, properties) }

  def addRelationship(startId: EntityId, relationshipType: String, endId: EntityId, properties: Map[String, CypherValue]): EntityId =
    flushAfter { inner.addRelationship(startId, relationshipType, endId, properties) }

  override def graph: PropertyGraph = graphCache match {
    case Some(cachedGraph) =>
      cachedGraph

    case None =>
      val newGraph = inner.graph
      graphCache = Some(newGraph)
      newGraph
  }

  override def clear(): Unit =
    flushAfter { inner.clear() }

  private def flushAfter[T](f: => T) =
    try {
      f
    } finally  {
      graphCache = None
    }
}
