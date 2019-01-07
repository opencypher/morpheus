/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
import org.opencypher.okapi.ir.api.expr.Expr

sealed trait ResultBlock extends Block {
  override val where: Set[Expr] = Set.empty
}

final case class TableResultBlock(
  after: List[Block],
  binds: OrderedFields,
  graph: IRGraph
) extends ResultBlock {

  def select(fields: Set[IRField]): TableResultBlock =
    copy(binds = binds.select(fields))
}

object TableResultBlock {
  def empty(graph: IRGraph) =
    TableResultBlock(List.empty, OrderedFields(), graph)
}

final case class OrderedFields(orderedFields: List[IRField] = List.empty) extends Binds {
  override def fields: Set[IRField] = orderedFields.toSet

  def select(fields: Set[IRField]): OrderedFields = copy(orderedFields = orderedFields.filter(fields.contains))
}

object OrderedFields {
  def fieldsFrom[E](fields: IRField*): OrderedFields = OrderedFields(fields.toList)

  def unapplySeq(arg: OrderedFields): Option[Seq[IRField]] = Some(arg.orderedFields)
}

final case class GraphResultBlock(
  after: List[Block],
  graph: IRGraph
) extends ResultBlock {
  override val binds: Binds = Binds.empty
}
