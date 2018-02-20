/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
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
