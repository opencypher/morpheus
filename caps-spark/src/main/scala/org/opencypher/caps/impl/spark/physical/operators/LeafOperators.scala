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
package org.opencypher.caps.impl.spark.physical.operators

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.impl.spark.CAPSRecords
import org.opencypher.caps.impl.spark.physical.{PhysicalResult, RuntimeContext}
import org.opencypher.caps.logical.impl.LogicalExternalGraph

private[spark] abstract class LeafPhysicalOperator extends PhysicalOperator {

  override def execute(implicit context: RuntimeContext): PhysicalResult = executeLeaf()

  def executeLeaf()(implicit context: RuntimeContext): PhysicalResult
}

final case class Start(records: CAPSRecords, graph: LogicalExternalGraph) extends LeafPhysicalOperator {

  override def executeLeaf()(implicit context: RuntimeContext): PhysicalResult =
    PhysicalResult(records, Map(graph.name -> resolve(graph.uri)))

}

final case class StartFromUnit(graph: LogicalExternalGraph)(implicit caps: CAPSSession)
  extends LeafPhysicalOperator {

  override def executeLeaf()(implicit context: RuntimeContext): PhysicalResult =
    PhysicalResult(CAPSRecords.unit(), Map(graph.name -> resolve(graph.uri)))

}
