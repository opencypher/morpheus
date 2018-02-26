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

import scala.language.implicitConversions

/**
  * Inter-graph operations between [[org.opencypher.caps.api.graph.PropertyGraph]]s.
  */
trait GraphOperations {

  self: PropertyGraph =>

  /**
    * Constructs the union of this graph and the argument graph. Note that the argument graph has to be managed by the
    * same session as this graph.
    *
    * @param other argument graph with which to union
    * @return union of this and the argument graph
    */
  // TODO: Explain semantics of the union (equality vs equivalence)
  def union(other: PropertyGraph): PropertyGraph
}
