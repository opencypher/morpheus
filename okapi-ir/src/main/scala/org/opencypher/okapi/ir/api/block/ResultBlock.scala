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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.ir.api.block

import org.opencypher.okapi.ir.api._

sealed trait ResultBlock[E] extends Block[E] {
  override val where: List[E] = List.empty
}

final case class TableResultBlock[E](
  after: List[Block[E]],
  binds: OrderedFields[E],
  graph: IRGraph
) extends ResultBlock[E] {

  def select(fields: Set[IRField]): TableResultBlock[E] =
    copy(binds = binds.select(fields))
}

object TableResultBlock {
  def empty[E](graph: IRGraph) =
    TableResultBlock(List.empty, OrderedFields[E](), graph)
}

final case class OrderedFields[E](orderedFields: List[IRField] = List.empty) extends Binds[E] {
  override def fields: Set[IRField] = orderedFields.toSet

  def select(fields: Set[IRField]): OrderedFields[E] = copy(orderedFields = orderedFields.filter(fields.contains))
}

object OrderedFields {
  def fieldsFrom[E](fields: IRField*): OrderedFields[E] = OrderedFields[E](fields.toList)

  def unapplySeq(arg: OrderedFields[_]): Option[Seq[IRField]] = Some(arg.orderedFields)
}

final case class GraphResultBlock[E](
  after: List[Block[E]],
  graph: IRGraph
) extends ResultBlock[E] {
  override val binds: Binds[E] = Binds.empty
}
