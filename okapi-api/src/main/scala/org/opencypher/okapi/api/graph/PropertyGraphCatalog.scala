package org.opencypher.okapi.api.graph

import org.opencypher.okapi.api.io.PropertyGraphDataSource

/**
  * The Catalog manages a sessions [[org.opencypher.okapi.api.io.PropertyGraphDataSource]]s.
  * Property graph data sources can be added and removed and queried during session runtime.
  */
trait PropertyGraphCatalog {

  //################################################
  // Property Graph Data Source specific functions
  //################################################

  /**
    * Returns all [[org.opencypher.okapi.api.graph.Namespace]]s registered at this catalog.
    *
    * @return registered namespaces
    */
  def namespaces: Set[Namespace]

  /**
    * Returns all available [[org.opencypher.okapi.api.io.PropertyGraphDataSource]]s.
    *
    * @return a map of all PGDS known to this catalog, keyed by their [[org.opencypher.okapi.api.graph.Namespace]]s.
    */
  def listSources: Map[Namespace, PropertyGraphDataSource]

  /**
    * Returns the [[org.opencypher.okapi.api.io.PropertyGraphDataSource]] that is registered under
    * the given [[org.opencypher.okapi.api.graph.Namespace]].
    *
    * @param namespace namespace for lookup
    * @return property graph data source
    */
  def source(namespace: Namespace): PropertyGraphDataSource

  /**
    * Register the given [[org.opencypher.okapi.api.io.PropertyGraphDataSource]] under
    * the specific [[org.opencypher.okapi.api.graph.Namespace]] within this catalog.
    *
    * This enables a user to refer to that [[org.opencypher.okapi.api.io.PropertyGraphDataSource]] within a Cypher query.
    *
    * Note, that it is not allowed to overwrite an already registered [[org.opencypher.okapi.api.graph.Namespace]].
    * Use [[org.opencypher.okapi.api.graph.PropertyGraphCatalog#deregisterSource]] first.
    *
    * @param namespace  namespace for lookup
    * @param dataSource property graph data source
    */
  def registerSource(namespace: Namespace, dataSource: PropertyGraphDataSource): Unit

  /**
    * De-registers a [[org.opencypher.okapi.api.io.PropertyGraphDataSource]] from the catalog
    * by its given [[org.opencypher.okapi.api.graph.Namespace]].
    *
    * @param namespace namespace for lookup
    */
  def deregisterSource(namespace: Namespace): Unit

  //################################################
  // Property Graph specific functions
  //################################################

  /**
    * Returns a set of [[org.opencypher.okapi.api.graph.QualifiedGraphName]]s for [[org.opencypher.okapi.api.graph.PropertyGraph]]s
    * that can be provided by this catalog.
    *
    * @return qualified names of graphs that can be provided
    */
  def graphNames: Set[QualifiedGraphName]

  /**
    * Stores the given [[org.opencypher.okapi.api.graph.PropertyGraph]] using
    * the [[org.opencypher.okapi.api.io.PropertyGraphDataSource]] registered under
    * the [[org.opencypher.okapi.api.graph.Namespace]] of the specified string representation
    * of a [[org.opencypher.okapi.api.graph.QualifiedGraphName]].
    *
    * @param qualifiedGraphName qualified graph name
    * @param graph              property graph to store
    */
  def store(qualifiedGraphName: String, graph: PropertyGraph): Unit =
    store(QualifiedGraphName(qualifiedGraphName), graph)

  /**
    * Stores the given [[org.opencypher.okapi.api.graph.PropertyGraph]] using
    * the [[org.opencypher.okapi.api.io.PropertyGraphDataSource]] registered under
    * the [[org.opencypher.okapi.api.graph.Namespace]] of the specified [[org.opencypher.okapi.api.graph.QualifiedGraphName]].
    *
    * @param qualifiedGraphName qualified graph name
    * @param graph              property graph to store
    */
  def store(qualifiedGraphName: QualifiedGraphName, graph: PropertyGraph): Unit

  /**
    * Removes the [[org.opencypher.okapi.api.graph.PropertyGraph]] associated with the given qualified graph name.
    *
    * @param qualifiedGraphName name of the graph within the session.
    */
  def delete(qualifiedGraphName: String): Unit =
    delete(QualifiedGraphName(qualifiedGraphName))

  /**
    * Removes the [[org.opencypher.okapi.api.graph.PropertyGraph]] with the given qualified name from the data source
    * associated with the specified [[org.opencypher.okapi.api.graph.Namespace]].
    *
    * @param qualifiedGraphName qualified graph name
    */
  def delete(qualifiedGraphName: QualifiedGraphName): Unit

  /**
    * Returns the [[org.opencypher.okapi.api.graph.PropertyGraph]] that is stored under the given
    * string representation of a [[org.opencypher.okapi.api.graph.QualifiedGraphName]].
    *
    * @param qualifiedGraphName qualified graph name
    * @return property graph
    */
  def graph(qualifiedGraphName: String): PropertyGraph =
    graph(QualifiedGraphName(qualifiedGraphName))

  /**
    * Returns the [[org.opencypher.okapi.api.graph.PropertyGraph]] that is stored under
    * the given [[org.opencypher.okapi.api.graph.QualifiedGraphName]].
    *
    * @param qualifiedGraphName qualified graph name
    * @return property graph
    */
  def graph(qualifiedGraphName: QualifiedGraphName): PropertyGraph

}
