/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.ir.api

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.ir.api.block.{Binds, Block}
import org.opencypher.okapi.ir.api.expr.Expr

sealed trait CypherStatement {
  def info: QueryInfo
}

sealed trait CypherQuery extends Block with CypherStatement

final case class SingleQuery(
    info: QueryInfo,
    model: QueryModel // TODO REMOVE
) extends CypherQuery {
  override def after: List[Block] = model.after

  override def binds: Binds = model.binds

  override def where: Set[Expr] = model.where

  override def graph: IRGraph = model.graph
}

final case class UnionAllQuery(
  left: CypherQuery,
  right: CypherQuery,
  info: QueryInfo
) extends CypherQuery {
  override def after: List[Block] = List(left, right)

  override def binds: Binds = left.binds

  override def where: Set[Expr] = Set.empty

  override def graph: IRGraph = right.graph
}

final case class CreateGraphStatement(
    info: QueryInfo,
    graph: IRGraph,
    innerQuery: SingleQuery
) extends CypherStatement

final case class CreateViewStatement(
  info: QueryInfo,
  qgn: QualifiedGraphName,
  parameterNames: List[String],
  innerQueryString: String
) extends CypherStatement


final case class DeleteGraphStatement(
  info: QueryInfo,
  qgn: QualifiedGraphName
) extends CypherStatement

final case class DeleteViewStatement(
  info: QueryInfo,
  qgn: QualifiedGraphName
) extends CypherStatement


