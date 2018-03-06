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
package org.opencypher.okapi.ir.api.block

import org.opencypher.okapi.ir.api._

final case class ProjectBlock[E](
    after: Set[BlockRef],
    binds: Fields[E] = Fields[E](),
    where: Set[E] = Set.empty[E],
    source: IRGraph,
    distinct: Boolean = false
) extends BasicBlock[Fields[E], E](BlockType("project"))

final case class Fields[E](items: Map[IRField, E] = Map.empty[IRField, E]) extends Binds[E] {
  override def fields: Set[IRField] = items.keySet
}

case object ProjectedFieldsOf {
  def apply[E](entries: (IRField, E)*) = Fields(entries.toMap)
}
