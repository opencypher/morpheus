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
package org.opencypher.caps.ir.api.block

import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.api.pattern.{AllGiven, AllOf}

final case class ResultBlock[E](
  after: Set[BlockRef],
  binds: OrderedFieldsAndGraphs[E],
  nodes: Set[IRField],
  relationships: Set[IRField],
  source: IRGraph,
  where: AllGiven[E] = AllGiven[E]()
) extends BasicBlock[OrderedFieldsAndGraphs[E], E](BlockType("result")) {

  def select(fields: Set[IRField]): ResultBlock[E] =
    copy(binds = binds.select(fields), nodes = nodes intersect fields, relationships = relationships intersect fields )
}

object ResultBlock {
  def empty[E](graph: IRGraph) = ResultBlock(Set.empty, OrderedFieldsAndGraphs[E](), Set.empty, Set.empty, graph, AllOf[E]())
}

final case class OrderedFieldsAndGraphs[E](
  fieldsOrder: IndexedSeq[IRField] = IndexedSeq.empty,
  override val graphs: Set[IRGraph] = Set.empty) extends Binds[E] {
  override def fields: Set[IRField] = fieldsOrder.toSet

  def select(fields: Set[IRField]): OrderedFieldsAndGraphs[E] =
    copy(fieldsOrder = fieldsOrder.filter(fields.contains))
}

case object FieldsInOrder {
  def apply[E](fields: IRField*): OrderedFieldsAndGraphs[E] = OrderedFieldsAndGraphs[E](fields.toIndexedSeq)
  def unapplySeq(arg: OrderedFieldsAndGraphs[_]): Option[Seq[IRField]] = Some(arg.fieldsOrder)
}

