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
package org.opencypher.caps.impl.physical

import org.opencypher.caps.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult}

object SparkCypherResultBuilder {
  def from(internal: PhysicalResult): SparkCypherResult = new SparkCypherResult {
    override def records: SparkCypherRecords = internal.records

    // TODO: Track which graph was the 'latest' used one
    override def graph: SparkCypherGraph = internal.graphs.head._2

    override def result(name: String): Option[SparkCypherResult] = internal.graphs.get(name).map { g =>
      new SparkCypherResult {
        override def records: SparkCypherRecords =
          throw new NotImplementedError("Records of stored intermediate result are not tracked!")

        override def graph: SparkCypherGraph = g

        override def result(name: String): Option[SparkCypherResult] = None
      }
    }
  }
}
