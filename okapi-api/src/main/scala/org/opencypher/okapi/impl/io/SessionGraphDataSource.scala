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
package org.opencypher.okapi.impl.io

import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.exception.{GraphAlreadyExistsException, GraphNotFoundException}

import scala.collection.mutable

object SessionGraphDataSource {

  val Namespace = org.opencypher.okapi.api.graph.Namespace("session")

}

class SessionGraphDataSource() extends PropertyGraphDataSource {

  private val graphMap: mutable.Map[GraphName, PropertyGraph] = mutable.Map.empty

  override def graph(name: GraphName): PropertyGraph =
    graphMap.getOrElse(name, throw GraphNotFoundException(s"Session graph with name `$name`."))

  override def schema(name: GraphName): Option[Schema] = Some(graph(name).schema)

  override def store(name: GraphName, graph: PropertyGraph): Unit = graphMap.get(name) match {
    case None => graphMap.update(name, graph)
    case Some(_) => throw GraphAlreadyExistsException(s"A graph with name $name is already stored in the session.")
  }

  override def delete(name: GraphName): Unit = graphMap.remove(name)

  override def graphNames: Set[GraphName] = graphMap.keySet.toSet

  override def hasGraph(name: GraphName): Boolean = graphMap.contains(name)
}
