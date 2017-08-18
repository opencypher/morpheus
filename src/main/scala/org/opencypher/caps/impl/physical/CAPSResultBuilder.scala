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

import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSResult}

object CAPSResultBuilder {
  def from(internal: PhysicalResult): CAPSResult = new CAPSResult {
    override def records: CAPSRecords = internal.records

    // TODO: Track which graph was the 'latest' used one
    override def graph: CAPSGraph = internal.graphs.head._2

    override def result(name: String): Option[CAPSResult] = internal.graphs.get(name).map { g =>
      new CAPSResult {
        override def records: CAPSRecords =
          throw new NotImplementedError("Records of stored intermediate result are not tracked!")

        override def graph: CAPSGraph = g

        override def result(name: String): Option[CAPSResult] = None
      }
    }
  }
}
