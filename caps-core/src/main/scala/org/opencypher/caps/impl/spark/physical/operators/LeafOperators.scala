/*
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

import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.impl.logical.LogicalExternalGraph

sealed trait LeafPhysicalOperator extends PhysicalOperator {
  override def execute(inputs: PhysicalResult*)(implicit context: RuntimeContext): PhysicalResult = {
    require(inputs.length == 0)
    executeLeaf()
  }

  def executeLeaf()(implicit context: RuntimeContext): PhysicalResult
}

final case class Start(records: CAPSRecords, graph: LogicalExternalGraph) extends LeafPhysicalOperator {
  override def executeLeaf()(implicit context: RuntimeContext): PhysicalResult = {
    PhysicalResult(records, Map(graph.name -> resolve(graph.uri)))
  }
}

final case class StartFrom(records: CAPSRecords, graph: LogicalExternalGraph) extends LeafPhysicalOperator {
  override def executeLeaf()(implicit context: RuntimeContext) = {
    PhysicalResult(records, Map(graph.name -> resolve(graph.uri)))
  }
}
