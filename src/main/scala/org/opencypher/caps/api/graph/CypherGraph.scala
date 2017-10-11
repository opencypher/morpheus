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
package org.opencypher.caps.api.graph

import org.opencypher.caps.api.record.CypherRecords
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.CypherValue

trait CypherGraph {

  self =>

  type Graph <: CypherGraph { type Records = self.Records }
  type Records <: CypherRecords { type Records = self.Records }
  type Session <: CypherSession { type Session = self.Session; type Graph = self.Graph; type Records = self.Records; type Result = self.Result }
  type Result <: CypherResult { type Graph = self.Graph; type Records = self.Records }

  // TODO: VerifiedSchema
  def schema: Schema

  def session: Session

  final def nodes(name: String): Records = nodes(name, CTNode)
  def nodes(name: String, nodeCypherType: CTNode): Records

  final def relationships(name: String): Records = relationships(name, CTRelationship)
  def relationships(name: String, relCypherType: CTRelationship): Records

  /**
    * Executes a Cypher query in the session that manages this graph, using this graph as the ambient graph.
    *
    * @param query      the Cypher query to execute.
    * @param parameters the parameters used by the Cypher query.
    * @return           the result of the query.
    * @see              [[CypherSession.cypher()]]
    */
  final def cypher(query: String, parameters: Map[String, CypherValue] = Map.empty): Result =
    session.cypher(graph, query, parameters)

  def union(other: Graph): Graph

  protected def graph: Graph
}
