package org.opencypher.okapi.impl.graph

import org.opencypher.okapi.api.graph.{Namespace, PropertyGraph, PropertyGraphCatalog, QualifiedGraphName}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.io.SessionGraphDataSource

/**
  * This is the default implementation of the [[org.opencypher.okapi.api.graph.PropertyGraphCatalog]].
  * It uses a mutable mapping to store the mapping between
  * [[org.opencypher.okapi.api.graph.Namespace]]s and [[org.opencypher.okapi.api.io.PropertyGraphDataSource]]s.
  *
  * By default this catalog mounts a single [[org.opencypher.okapi.impl.io.SessionGraphDataSource]] under the namespace
  * [[org.opencypher.okapi.impl.graph.CypherCatalog#sessionNamespace]]. This is PGDS is used to session local graphs.
  */
class CypherCatalog extends PropertyGraphCatalog{

  /**
    * The [[org.opencypher.okapi.api.graph.Namespace]] used to to store graphs within this session.
    *
    * @return session namespace
    */
  def sessionNamespace: Namespace = SessionGraphDataSource.Namespace

  /**
    * Stores a mutable mapping between a data source [[org.opencypher.okapi.api.graph.Namespace]] and the specific
    * [[org.opencypher.okapi.api.io.PropertyGraphDataSource]].
    *
    * This mapping also holds the [[org.opencypher.okapi.impl.io.SessionGraphDataSource]] by default.
    */
  private var dataSourceMapping: Map[Namespace, PropertyGraphDataSource] =
    Map(sessionNamespace -> new SessionGraphDataSource)

  override def namespaces: Set[Namespace] = dataSourceMapping.keySet

  override def source(namespace: Namespace): PropertyGraphDataSource = dataSourceMapping.getOrElse(namespace,
    throw IllegalArgumentException(s"a data source registered with namespace '$namespace'"))

  override def listSources: Map[Namespace, PropertyGraphDataSource] = dataSourceMapping

  override def registerSource(namespace: Namespace, dataSource: PropertyGraphDataSource): Unit = dataSourceMapping.get(namespace) match {
    case Some(p) => throw IllegalArgumentException(s"no data source registered with namespace '$namespace'", p)
    case None => dataSourceMapping = dataSourceMapping.updated(namespace, dataSource)
  }

  override def deregisterSource(namespace: Namespace): Unit = {
    if (namespace == sessionNamespace) throw UnsupportedOperationException("de-registering the session data source")
    dataSourceMapping.get(namespace) match {
      case Some(_) => dataSourceMapping = dataSourceMapping - namespace
      case None => throw IllegalArgumentException(s"a data source registered with namespace '$namespace'")
    }
  }

  override def graphNames: Set[QualifiedGraphName] =
    dataSourceMapping.flatMap {
      case (namespace, pgds) =>
        pgds.graphNames.map(n => QualifiedGraphName(namespace, n))
    }.toSet

  override def store(qualifiedGraphName: QualifiedGraphName, graph: PropertyGraph): Unit =
    source(qualifiedGraphName.namespace).store(qualifiedGraphName.graphName, graph)

  override def delete(qualifiedGraphName: QualifiedGraphName): Unit =
    source(qualifiedGraphName.namespace).delete(qualifiedGraphName.graphName)

  override def graph(qualifiedGraphName: QualifiedGraphName): PropertyGraph =
    source(qualifiedGraphName.namespace).graph(qualifiedGraphName.graphName)

}
