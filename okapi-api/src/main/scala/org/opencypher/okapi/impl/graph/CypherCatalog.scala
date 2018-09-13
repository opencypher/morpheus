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
package org.opencypher.okapi.impl.graph

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.value.CypherValue.CypherString
import org.opencypher.okapi.impl.annotations.experimental
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.graph.FromGraphParser._
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.v9_0.ast.{FromGraph, ViewInvocation}

/**
  * This is the default implementation of the [[org.opencypher.okapi.api.graph.PropertyGraphCatalog]].
  * It uses a mutable mapping to store the mapping between
  * [[org.opencypher.okapi.api.graph.Namespace]]s and [[org.opencypher.okapi.api.io.PropertyGraphDataSource]]s.
  *
  * By default this catalog mounts a single [[org.opencypher.okapi.impl.io.SessionGraphDataSource]] under the namespace
  * [[org.opencypher.okapi.impl.graph.CypherCatalog#sessionNamespace]]. This PGDS is used to store session local graphs.
  */
class CypherCatalog extends PropertyGraphCatalog {

  /**
    * The [[org.opencypher.okapi.api.graph.Namespace]] used to store graphs within this session.
    *
    * @return session namespace
    */
  def sessionNamespace: Namespace = SessionGraphDataSource.Namespace

  /**
    * Stores a mutable mapping between a [[org.opencypher.okapi.api.graph.Namespace]] and the specific
    * [[org.opencypher.okapi.api.io.PropertyGraphDataSource]].
    *
    * This mapping also holds the [[org.opencypher.okapi.impl.io.SessionGraphDataSource]] by default.
    */
  private var dataSourceMapping: Map[Namespace, PropertyGraphDataSource] =
    Map(sessionNamespace -> new SessionGraphDataSource)

  private var viewMapping: Map[QualifiedGraphName, ParameterizedView] = Map.empty

  override def namespaces: Set[Namespace] = dataSourceMapping.keySet

  override def source(namespace: Namespace): PropertyGraphDataSource = dataSourceMapping.getOrElse(namespace,
    throw IllegalArgumentException(s"a data source registered with namespace '$namespace'"))

  override def listSources: Map[Namespace, PropertyGraphDataSource] = dataSourceMapping

  override def register(
    namespace: Namespace,
    dataSource: PropertyGraphDataSource
  ): Unit = dataSourceMapping.get(namespace) match {
    case Some(p) => throw IllegalArgumentException(s"There is already a data source registered with namespace '$namespace'", p)
    case None => dataSourceMapping = dataSourceMapping.updated(namespace, dataSource)
  }

  override def deregister(namespace: Namespace): Unit = {
    if (namespace == sessionNamespace) throw UnsupportedOperationException("de-registering the session data source")
    dataSourceMapping.get(namespace) match {
      case Some(_) => dataSourceMapping = dataSourceMapping - namespace
      case None => throw IllegalArgumentException(s"No data source registered with namespace '$namespace'")
    }
  }

  override def graphNames: Set[QualifiedGraphName] = {
    dataSourceMapping.flatMap {
      case (namespace, pgds) =>
        pgds.graphNames.map(n => QualifiedGraphName(namespace, n))
    }.toSet
  }

  override def viewNames: Set[QualifiedGraphName] = viewMapping.keySet

  override def store(qualifiedGraphName: QualifiedGraphName, graph: PropertyGraph): Unit =
    source(qualifiedGraphName.namespace).store(qualifiedGraphName.graphName, graph)

  override def store(qualifiedGraphName: QualifiedGraphName, parameters: List[String], viewQuery: String): Unit = {
    // TODO: Add tests and throw exceptions when there are QGN collisions.
    viewMapping += (qualifiedGraphName -> ParameterizedView(parameters, viewQuery))
  }

  override def delete(qualifiedGraphName: QualifiedGraphName): Unit =
    source(qualifiedGraphName.namespace).delete(qualifiedGraphName.graphName)

  override def graph(qualifiedGraphName: QualifiedGraphName): PropertyGraph =
    source(qualifiedGraphName.namespace).graph(qualifiedGraphName.graphName)

  // TODO: Error handling
  override def view(
    qualifiedGraphName: QualifiedGraphName,
    parameters: List[CypherString] = Nil
  )(implicit session: CypherSession): PropertyGraph = {
    val viewDefinition = viewMapping(qualifiedGraphName)
    val paramNameValueTuples = viewDefinition.parameterNames.zip(parameters)
    val parsedParameterTuples = paramNameValueTuples.map { case (name, stringValue) =>
      name -> FromGraphParser.parse(name, stringValue.value)
    }
    val (parameterMap, queryLocalGraphs) = parsedParameterTuples.foldLeft(Map.empty[String, CypherString] -> Map.empty[QualifiedGraphName, PropertyGraph]) {
      case ((currentParamMap, currentQueryLocalGraphs), (nextName, nextFrom: FromGraph)) =>
      nextFrom match {
        case ViewInvocation(catalogName, params) => // Recursive view evaluation
          val graph = view(QualifiedGraphName(catalogName.parts), params.map(_.toCypherString).toList)
          val graphQgn = session.generateQualifiedGraphName
          currentParamMap.updated(nextName, CypherString(graphQgn.toString)) -> currentQueryLocalGraphs.updated(graphQgn, graph)
        case _ => // Simple case, parameter is just passed on
          currentParamMap.updated(nextName, nextFrom.toCypherString) -> currentQueryLocalGraphs
      }
    }
    session.cypher(viewDefinition.viewQuery, parameterMap, queryCatalog = queryLocalGraphs).graph
  }

}

// TODO: Allow for typed parameters
@experimental
case class ParameterizedView(parameterNames: List[String], viewQuery: String)
