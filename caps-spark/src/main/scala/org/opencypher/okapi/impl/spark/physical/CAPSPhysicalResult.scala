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
package org.opencypher.okapi.impl.spark.physical

import org.opencypher.okapi.api.CAPSSession
import org.opencypher.okapi.impl.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.okapi.relational.api.physical.PhysicalResult

object CAPSPhysicalResult {
  def unit(implicit caps: CAPSSession) = CAPSPhysicalResult(CAPSRecords.unit(), Map.empty)
}

case class CAPSPhysicalResult(records: CAPSRecords, graphs: Map[String, CAPSGraph])
  extends PhysicalResult[CAPSRecords, CAPSGraph] {

  override def mapRecordsWithDetails(f: CAPSRecords => CAPSRecords): CAPSPhysicalResult =
    copy(records = f(records))
  override def withGraph(t: (String, CAPSGraph)): CAPSPhysicalResult =
    copy(graphs = graphs.updated(t._1, t._2))
  override def selectGraphs(selected: Set[String]): CAPSPhysicalResult =
    copy(graphs = graphs.filterKeys(selected))
}

