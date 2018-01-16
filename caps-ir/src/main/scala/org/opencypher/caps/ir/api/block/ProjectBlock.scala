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
package org.opencypher.caps.ir.api.block

import org.opencypher.caps.api.schema.AllGiven
import org.opencypher.caps.ir.api._

final case class ProjectBlock[E](
    after: Set[BlockRef],
    binds: FieldsAndGraphs[E] = FieldsAndGraphs[E](),
    where: AllGiven[E] = AllGiven[E](),
    source: IRGraph,
    distinct: Boolean = false
) extends BasicBlock[FieldsAndGraphs[E], E](BlockType("project"))

final case class FieldsAndGraphs[E](
    items: Map[IRField, E] = Map.empty[IRField, E],
    override val graphs: Set[IRGraph] = Set.empty)
    extends Binds[E] {
  override def fields: Set[IRField] = items.keySet
}

case object ProjectedFieldsOf {
  def apply[E](entries: (IRField, E)*) = FieldsAndGraphs(entries.toMap)
}
