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
package org.opencypher.caps.api.ir.block

import org.opencypher.caps.api.ir.Field
import org.opencypher.caps.api.ir.pattern.AllGiven

final case class AggregationBlock[E](
    after: Set[BlockRef],
    binds: Aggregations[E],
    group: Set[Field]
) extends BasicBlock[Aggregations[E], E](BlockType("aggregation")) {

  override val where: AllGiven[E] = AllGiven[E]() // no filtering in aggregation blocks
  override def graph: BlockRef = after.head // this will be removed
}

final case class Aggregations[E](pairs: Set[(Field, E)]) extends Binds[E] {
  override def fields = pairs.map(_._1)
}
