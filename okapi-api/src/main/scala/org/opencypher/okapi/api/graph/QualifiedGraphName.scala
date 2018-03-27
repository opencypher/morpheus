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
package org.opencypher.okapi.api.graph

import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.io.SessionPropertyGraphDataSource

/**
  * A graph name is used to address a specific graph within a [[Namespace]] and is used for lookups in the
  * [[org.opencypher.okapi.api.graph.CypherSession]].
  *
  * @param value string representing the graph name
  */
case class GraphName(value: String) extends AnyVal {
  override def toString: String = value
}

/**
  * A namespace is used to address different [[org.opencypher.okapi.api.io.PropertyGraphDataSource]] implementations within a
  * [[org.opencypher.okapi.api.graph.CypherSession]].
  *
  * @param value string representing the namespace
  */
case class Namespace(value: String) extends AnyVal {
  override def toString: String = value
}

object QualifiedGraphName {

  /**
    * Returns a [[org.opencypher.okapi.api.graph.QualifiedGraphName]] from its string representation. A qualified graph name consists of a namespace
    * part and a graph name part separated by a '.' character. For example,
    *
    * {{{
    *   mynamespace.mygraphname
    *   mynamespace.my.graph.name
    * }}}
    *
    * are valid qualified graph names. The separation between namespace and graph name is expected to be at the first
    * occurring '.'. A graph name may contain an arbitrary number of additional '.' characters. Note that a string
    * without any '.' characters is considered to be associated with the [[org.opencypher.okapi.impl.io.SessionPropertyGraphDataSource]].
    *
    * @param qualifiedGraphName string representation of a qualified graph name
    * @return qualified graph name
    */
  def apply(qualifiedGraphName: String): QualifiedGraphName = apply(qualifiedGraphName.split("\\.").toList)

  private[okapi] def apply(parts: List[String]): QualifiedGraphName = parts match {
    case Nil => throw IllegalArgumentException("qualified graph name or single graph name")
    case head :: Nil => QualifiedGraphName(SessionPropertyGraphDataSource.Namespace, GraphName(head))
    case head :: tail => QualifiedGraphName(Namespace(head), GraphName(tail.mkString(".")))
  }

}

/**
  * A qualified graph name is used in a Cypher query to address a specific graph within a namespace.
  *
  * Example:
  *
  * {{{
  * FROM GRAPH myNamespace.myGraphName MATCH (n) RETURN n
  * }}}
  *
  * Here, {{myNamespace.myGraphName}} represents a qualified graph name.
  *
  * @param namespace namespace part of the qualified graph name
  * @param graphName graph name part of the qualified graph name
  */
case class QualifiedGraphName(namespace: Namespace, graphName: GraphName) {
  override def toString: String = s"$namespace.$graphName"
}

