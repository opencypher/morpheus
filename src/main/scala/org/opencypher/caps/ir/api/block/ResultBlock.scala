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

import java.net.URI

import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.api.pattern.{AllGiven, AllOf}

final case class ResultBlock[E](
  after: Set[BlockRef],
  binds: OrderedFields[E],
  nodes: Set[IRField],
  relationships: Set[IRField],
  source: NamedGraph,
  where: AllGiven[E] = AllGiven[E]()
) extends BasicBlock[OrderedFields[E], E](BlockType("result")) {

  def select(fields: Set[IRField]): ResultBlock[E] =
    copy(binds = binds.select(fields), nodes = nodes intersect fields, relationships = relationships intersect fields )
}

object ResultBlock {
  def empty[E](graph: NamedGraph) = ResultBlock(Set.empty, OrderedFields[E](), Set.empty, Set.empty, graph, AllOf[E]())
}

final case class OrderedFields[E](fieldsOrder: IndexedSeq[IRField] = IndexedSeq.empty) extends Binds[E] {
  override def fields = fieldsOrder.toSet

  def select(fields: Set[IRField]): OrderedFields[E] =
    copy(fieldsOrder = fieldsOrder.filter(fields.contains))
}

case object FieldsInOrder {
  def apply[E](fields: IRField*) = OrderedFields[E](fields.toIndexedSeq)
  def unapplySeq(arg: OrderedFields[_]): Option[Seq[IRField]] = Some(arg.fieldsOrder)
}

