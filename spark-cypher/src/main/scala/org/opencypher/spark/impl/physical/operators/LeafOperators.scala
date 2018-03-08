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
package org.opencypher.spark.impl.physical.operators

import org.opencypher.okapi.logical.impl.LogicalCatalogGraph
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}

private[spark] abstract class LeafPhysicalOperator extends CAPSPhysicalOperator {

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = executeLeaf()

  def executeLeaf()(implicit context: CAPSRuntimeContext): CAPSPhysicalResult
}

final case class Start(records: CAPSRecords, graph: LogicalCatalogGraph) extends LeafPhysicalOperator {

  override val header = records.header

  override def executeLeaf()(implicit context: CAPSRuntimeContext): CAPSPhysicalResult =
    CAPSPhysicalResult(records, resolve(graph.qualifiedGraphName))

}

final case class StartFromUnit(graph: LogicalCatalogGraph)(implicit caps: CAPSSession)
  extends LeafPhysicalOperator {

  override val header = RecordHeader.empty

  override def executeLeaf()(implicit context: CAPSRuntimeContext): CAPSPhysicalResult =
    CAPSPhysicalResult(CAPSRecords.unit(), resolve(graph.qualifiedGraphName))

}
