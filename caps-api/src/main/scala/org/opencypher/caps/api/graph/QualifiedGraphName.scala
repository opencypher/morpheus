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
package org.opencypher.caps.api.graph

import org.opencypher.caps.api.io.PropertyGraphDataSource

// TODO: Remove companions and use normal case classes
object GraphName {
  def from(graphName: String) = GraphName(graphName)
}

/**
  * A graph name is used to address a specific graph withing a [[Namespace]] and is used for lookups in the
  * [[org.opencypher.caps.api.graph.CypherSession]].
  *
  * @param value string representing the graph name
  */
case class GraphName private(value: String) extends AnyVal {
  override def toString: String = value
}

object Namespace {
  def from(namespace: String) = Namespace(namespace)
}

/**
  * A namespace is used to address different [[PropertyGraphDataSource]] implementations within a
  * [[org.opencypher.caps.api.graph.CypherSession]].
  *
  * @param value string representing the namespace
  */
case class Namespace private(value: String) extends AnyVal {
  override def toString: String = value
}

object QualifiedGraphName {
  def from(namespace: String, graphName: String) =
    QualifiedGraphName(Namespace.from(namespace), GraphName.from(graphName))
}
/**
  * A qualified graph name is used in a Cypher query to address a specific graph within a namespace.
  *
  * Example:
  *
  * {{{
  * FROM GRAPH AT 'myNamespace.myGraphName' MATCH (n) RETURN n
  * }}}
  *
  * Here, {{myNamespace.myGraphName}} represents a qualified graph name.
  *
  * @param namespace namespace part of the qualified graph name
  * @param graphName graph name part of the qualified graph name
  */
case class QualifiedGraphName private(namespace: Namespace, graphName: GraphName) {
  override def toString: String = s"$namespace.$graphName"
}

