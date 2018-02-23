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
package org.opencypher.caps.logical.impl

import org.opencypher.caps.api.io.PropertyGraphDataSource
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.ir.api.IRGraph
import org.opencypher.caps.ir.api.expr.Var

final case class LogicalPlannerContext(
  ambientGraphSchema: Schema,
  inputRecordFields: Set[Var],
  resolver: String => PropertyGraphDataSource,
  sourceGraph: IRGraph
) {
  def withSourceGraph(graph: IRGraph): LogicalPlannerContext = copy(sourceGraph = graph)
}
