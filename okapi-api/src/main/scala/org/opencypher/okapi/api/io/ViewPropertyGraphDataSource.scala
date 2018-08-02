package org.opencypher.okapi.api.io

import org.opencypher.okapi.api.graph.{CypherSession, GraphName, PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.exception.UnsupportedOperationException

/**
  * This data source applies a view query to another graph in the session catalog.
  *
  * It interprets the graph name passed to it as the qualified graph name of the underlying graph to which the view is
  * applied.
  *
  * @param viewQuery query applied to the underlying graph
  * @param customGraphNameMappings allows to customise the mapping of view names to QGNs
  */
case class ViewPropertyGraphDataSource(
  viewQuery: String,
  customGraphNameMappings: Map[GraphName, QualifiedGraphName] = Map.empty
)(implicit session: CypherSession)
  extends PropertyGraphDataSource {

  /**
    * Converts a graph name to the qualified graph name of the underlying graph.
    */
  protected def underlyingGraphQgn(name: GraphName): QualifiedGraphName = {
    customGraphNameMappings.getOrElse(name,  QualifiedGraphName(name.value))
  }

  /**
    * Returns the underlying graph.
    */
  protected def underlyingGraph(name: GraphName): PropertyGraph = session.catalog.graph(underlyingGraphQgn(name))

  override def hasGraph(name: GraphName): Boolean = {
    val ds = session.catalog.source(underlyingGraphQgn(name).namespace) // Data source that manages the underlying graph
    ds.hasGraph(underlyingGraphQgn(name).graphName)
  }

  override def graph(name: GraphName): PropertyGraph = {
    val graphToConstructViewOn = underlyingGraph(name)
    graphToConstructViewOn.cypher(viewQuery).graph
  }

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit =
    throw UnsupportedOperationException("Storing a view is not supported")

  override def delete(name: GraphName): Unit =
    throw UnsupportedOperationException("Deleting a view is not supported")

  /**
    * Returns all graph names managed by other data sources and returns their name as it would be passed to this DS.
    */
  override val graphNames: Set[GraphName] = {
    session.catalog.listSources.filterNot(_._2 == this).flatMap { case (namespace, ds) =>
      ds.graphNames.map(originalName => GraphName(s"$namespace.${originalName.value}"))
    }.toSet ++ customGraphNameMappings.keys
  }

}
