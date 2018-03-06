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

import org.opencypher.okapi.ir.api.IRGraph

final case class OrderAndSliceBlock[E](
    after: Set[BlockRef],
    orderBy: Seq[SortItem[E]],
    skip: Option[E],
    limit: Option[E],
    source: IRGraph
) extends BasicBlock[OrderedFields[E], E](BlockType("order-and-slice")) {
  override val binds = OrderedFields[E]()
  override def where: Set[E] = Set.empty[E]
}

sealed trait SortItem[E] {
  def expr: E
}

final case class Asc[E](expr: E) extends SortItem[E]
final case class Desc[E](expr: E) extends SortItem[E]
