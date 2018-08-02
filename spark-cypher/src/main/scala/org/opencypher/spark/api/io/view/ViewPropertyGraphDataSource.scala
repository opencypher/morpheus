/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.api.io.view

import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.spark.api.CAPSSession

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
)(implicit session: CAPSSession)
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
