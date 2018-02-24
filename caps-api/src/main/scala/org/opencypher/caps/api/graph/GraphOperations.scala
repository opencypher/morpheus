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

import org.opencypher.caps.impl.exception.UnsupportedOperationException

import scala.language.implicitConversions

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
