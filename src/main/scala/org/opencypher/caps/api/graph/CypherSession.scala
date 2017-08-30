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

import java.net.URI

import org.opencypher.caps.api.record.CypherRecords
import org.opencypher.caps.api.value.CypherValue

trait CypherSession {

  self =>

  type Graph <: CypherGraph { type Graph = self.Graph; type Records = self.Records }
  type Session <: CypherSession { type Session = self.Session; type Graph = self.Graph; type Records = self.Records; type Result = self.Result; type Data = self.Data }
  type Records <: CypherRecords { type Records = self.Records; type Data = self.Data }
  type Result <: CypherResult { type Result = self.Result; type Graph = self.Graph; type Records = self.Records }
  type Data

  def cypher(graph: Graph, query: String, parameters: Map[String, CypherValue]): Result
  def withGraphAt(uri: URI, alias: String): Graph
}


