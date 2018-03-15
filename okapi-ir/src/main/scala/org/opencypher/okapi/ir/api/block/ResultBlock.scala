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

sealed trait ResultBlock[E] extends Block[E] {
  override val where: Set[E] = Set.empty
}

final case class TableResultBlock[E](
  after: Set[BlockRef],
  binds: OrderedFields[E],
  nodes: Set[IRField],
  relationships: Set[IRField],
  graph: IRGraph
) extends ResultBlock[E] {

  def select(fields: Set[IRField]): TableResultBlock[E] =
    copy(binds = binds.select(fields), nodes = nodes intersect fields, relationships = relationships intersect fields)
}

object TableResultBlock {
  def empty[E](graph: IRGraph) =
    TableResultBlock(Set.empty, OrderedFields[E](), Set.empty, Set.empty, graph)
}

final case class OrderedFields[E](fieldsOrder: IndexedSeq[IRField] = IndexedSeq.empty) extends Binds[E] {
  override def fields: Set[IRField] = fieldsOrder.toSet

  def select(fields: Set[IRField]): OrderedFields[E] = copy(fieldsOrder = fieldsOrder.filter(fields.contains))
}

object OrderedFields {
  def fieldsFrom[E](fields: IRField*): OrderedFields[E] = OrderedFields[E](fields.toIndexedSeq)

  def unapplySeq(arg: OrderedFields[_]): Option[Seq[IRField]] = Some(arg.fieldsOrder)
}

final case class GraphResultBlock[E](
  after: Set[BlockRef],
  graph: IRGraph
) extends ResultBlock[E] {
  override val binds: Binds[E] = Binds.empty
}
