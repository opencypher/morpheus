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
package org.opencypher.spark.api.ir.block

import org.opencypher.spark.api.ir._
import org.opencypher.spark.api.ir.pattern.{AllGiven, AllOf}

final case class ResultBlock[E](
  after: Set[BlockRef],
  binds: OrderedFields[E],
  nodes: Set[Field],
  relationships: Set[Field],
  graph: BlockRef,
  where: AllGiven[E] = AllGiven[E]()
) extends BasicBlock[OrderedFields[E], E](BlockType("result")) {

  def select(fields: Set[Field]): ResultBlock[E] =
    copy(binds = binds.select(fields), nodes = nodes intersect fields, relationships = relationships intersect fields )
}

object ResultBlock {
  def empty[E](graphBlock: BlockRef) = ResultBlock(Set.empty, OrderedFields[E](), Set.empty, Set.empty, graphBlock, AllOf[E]())
}

final case class OrderedFields[E](fieldsOrder: IndexedSeq[Field] = IndexedSeq.empty) extends Binds[E] {
  override def fields = fieldsOrder.toSet

  def select(fields: Set[Field]): OrderedFields[E] =
    copy(fieldsOrder = fieldsOrder.filter(fields.contains))
}

case object FieldsInOrder {
  def apply[E](fields: Field*) = OrderedFields[E](fields.toIndexedSeq)
  def unapplySeq(arg: OrderedFields[_]): Option[Seq[Field]] = Some(arg.fieldsOrder)
}

