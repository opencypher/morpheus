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
package org.opencypher.caps.cosc.impl.planning

import org.opencypher.caps.cosc.impl
import org.opencypher.caps.cosc.impl.{COSCPhysicalResult, COSCRecords, COSCRuntimeContext}
import org.opencypher.caps.impl.table.RecordHeader
import org.opencypher.caps.logical.impl.LogicalExternalGraph

abstract class LeafOperator extends COSCOperator

case class Start(records: COSCRecords, graph: LogicalExternalGraph) extends LeafOperator {

  override val header: RecordHeader = records.header

  override def execute(implicit context: COSCRuntimeContext): COSCPhysicalResult =
    impl.COSCPhysicalResult(records, Map(graph.name -> resolve(graph.qualifiedGraphName)))
}
