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
package org.opencypher.spark.api.graph

import org.opencypher.spark.api.classes.Cypher
import org.opencypher.spark.api.record.CypherRecords

trait GraphSpace {

  self =>

  type Space <: GraphSpace { type Space = self.Space; type Records = self.Records; type Graph = self.Graph }
  type Engine <: Cypher { type Space = self.Space; type Record = self.Records; type Graph = self.Graph }
  type Records <: CypherRecords { type Records = self.Records }
  type Graph <: CypherGraph { type Space = self.Space; type Graph = self.Graph; type Records = self.Records }

  def base: Graph
  def engine: Engine
}


