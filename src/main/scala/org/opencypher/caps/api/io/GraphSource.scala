/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
/**
  * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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

import java.net.URI

import org.opencypher.caps.api.graph.{CypherGraph, CypherSession}
import org.opencypher.caps.api.schema.Schema

trait GraphSource {

  self =>

  type Session <: CypherSession { type Session = self.Session; type Graph = self.Graph }
  type Graph <: CypherGraph { type Session = self.Session; type Graph = self.Graph }

  /**
    * The session tied to this graph source.
    */
  val session: Session

  /**
    * Determines whether this is a source for a graph at the argument uri.
    *
    * @param uri the location for a potential graph.
    * @return true if this graph source is located at the argument uri.
    */
  def sourceForGraphAt(uri: URI): Boolean

  /**
    * A canonical uri describing the location of this graph source.
    * The sourceForGraphAt function is guaranteed to return true for this uri.
    *
    * @return a uri describing the location of this graph source.
    */
  def canonicalURI: URI

  /**
    * Create a new empty graph stored in this graph source.
    *
    * @return the graph stored in this graph source.
    * @throws RuntimeException if the graph could not be created or there already was a graph
    */
  def create: Graph

  /**
    * Provides the graph stored in this graph source.
    *
    * @return the graph stored in this graph source.
    * @throws RuntimeException if loading the graph could not be done.
    */
  def graph: Graph

  /**
    * Provides only the schema of the graph stored in this graph source or returns None if the schema cannot be
    * provided without loading/constructing the whole graph.
    *
    * @return the schema of the graph stored in this graph source.
    */
  def schema: Option[Schema]

  /**
    * Persists the argument graph to this source.
    *
    * @param mode the persist mode to use.
    * @param graph the graph to persist.
    * @return the persisted graph
    */
  def persist(mode: PersistMode, graph: Graph): Graph

  /**
    * Delete the graph stored at this graph source
    */
  def delete(): Unit
}
