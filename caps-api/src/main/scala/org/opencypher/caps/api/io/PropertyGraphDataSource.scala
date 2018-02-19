package org.opencypher.caps.api.io

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.Schema

trait PropertyGraphDataSource {

  def graph(name: GraphName): PropertyGraph

  def schema(name: GraphName): Option[Schema]

  def store(name: GraphName, graph: PropertyGraph): Unit

  def delete(name: GraphName): Unit

  def graphNames: Set[GraphName]

}

case class GraphName(graphName: String) extends AnyVal

case class Namespace(namespace: String) extends AnyVal

case class GraphIdentifier(namespace: Namespace, graphName: GraphName) {
  override def toString: String = s"$namespace.$graphName"
}
