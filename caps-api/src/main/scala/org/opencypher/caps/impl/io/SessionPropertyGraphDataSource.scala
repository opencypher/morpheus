package org.opencypher.caps.impl.io

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.{GraphName, PropertyGraphDataSource}
import org.opencypher.caps.api.schema.Schema

object SessionPropertyGraphDataSource {
  val Namespace = org.opencypher.caps.api.io.Namespace("session")
}

class SessionPropertyGraphDataSource() extends PropertyGraphDataSource {

  private var graphMap: Map[GraphName, PropertyGraph] = Map.empty

  override def graph(name: GraphName): PropertyGraph =
    graphMap(name)

  override def schema(name: GraphName): Option[Schema] =
    Some(graphMap(name).schema)

  override def store(name: GraphName, graph: PropertyGraph): Unit =
    graphMap = graphMap.updated(name, graph)

  override def delete(name: GraphName): Unit =
  // TODO: uncache graph
    graphMap = graphMap.filterKeys(_ != name)

  override def graphNames: Set[GraphName] =
    graphMap.keySet
}
