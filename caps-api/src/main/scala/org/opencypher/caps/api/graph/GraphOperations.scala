package org.opencypher.caps.api.graph

import org.opencypher.caps.impl.exception.UnsupportedOperationException

/**
  * Mixin for adding additional operations to a [[PropertyGraph]].
  */
trait GraphOperations {

  self: PropertyGraph =>

  /**
    * Constructs the union of this graph and the argument graph. Note that, the argument graph has to be managed by the
    * same session as this graph.
    *
    * @param other argument graph with which to union
    * @return union of this and the argument graph
    */
  def union(other: PropertyGraph): PropertyGraph
}

object GraphOperations {

  /**
    * Converts a [[PropertyGraph]] into a [[PropertyGraph]] with [[GraphOperations]] if it is supported.
    *
    * @param graph property graph
    * @return property graph with graph operations
    */
  implicit def withOperations(graph: PropertyGraph): PropertyGraph with GraphOperations = graph match {
    case g: GraphOperations => g
    case g => throw UnsupportedOperationException(s"graph operations on unsupported graph: $g")
  }
}
