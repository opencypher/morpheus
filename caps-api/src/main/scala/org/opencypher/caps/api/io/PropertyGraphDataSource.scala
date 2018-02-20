package org.opencypher.caps.api.io

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.Schema

trait PropertyGraphDataSource {

  def graph(name: GraphName): PropertyGraph

  def schema(name: GraphName): Option[Schema]

  def store(name: GraphName, graph: PropertyGraph): Unit

  def delete(name: GraphName): Unit

  // TODO: necessary?
  def graphNames: Set[GraphName]

}

object GraphName {
  def create(graphName: String) = GraphName(graphName)
}

case class GraphName(value: String) extends AnyVal {
  override def toString: String = value
}

object Namespace {
  def create(namespace: String) = Namespace(namespace)
}

case class Namespace(value: String) extends AnyVal {
  override def toString: String = value
}

object QualifiedGraphName {
  def create(namespace: String, graphName: String) =
    QualifiedGraphName(Namespace.create(namespace), GraphName.create(graphName))
}

case class QualifiedGraphName(namespace: Namespace, graphName: GraphName) {
  override def toString: String = s"$namespace.$graphName"
}
