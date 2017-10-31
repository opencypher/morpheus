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
package org.opencypher.caps.impl.spark.physical

import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}

case class PhysicalResult(records: CAPSRecords, graphs: Map[String, CAPSGraph]) {
  def mapRecordsWithDetails(f: CAPSRecords => CAPSRecords): PhysicalResult =
    copy(records = f(records))
  def withGraph(t: (String, CAPSGraph)): PhysicalResult =
    copy(graphs = graphs.updated(t._1, t._2))
  def selectGraphs(selected: Set[String]): PhysicalResult =
    copy(graphs = graphs.filterKeys(selected))
}

