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
package org.opencypher.okapi.relational.api.planning

import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}
import org.opencypher.okapi.relational.impl.operators.{RelationalOperator, ReturnGraph, Select}

case class RelationalCypherResult[T <: Table[T]](
  maybeLogical: Option[LogicalOperator],
  maybeRelational: Option[RelationalOperator[T]]
)(implicit session: RelationalCypherSession[T]) extends CypherResult {

  override type Graph = RelationalCypherGraph[T]

  override def getGraph: Option[Graph] = maybeRelational.flatMap {
    case r: ReturnGraph[T] => Some(r.graph)
    case _ => None
  }

  override def getRecords: Option[RelationalCypherRecords[T]] =
    maybeRelational.flatMap {
      case s: Select[T] => Some(session.records.from(s.header, s.table, s.returnItems.map(_.map(_.name))))
      case _ => None
    }

  override def show(implicit options: PrintOptions): Unit = getRecords match {
    case Some(r) => r.show
    case None => options.stream.print("No results")
  }

  override def plans: QueryPlans[T] = QueryPlans(maybeLogical, maybeRelational)
}

object RelationalCypherResult {

  def empty[T <: Table[T]](implicit session: RelationalCypherSession[T]): RelationalCypherResult[T] =
    RelationalCypherResult(None, None)

  def apply[T <: Table[T]](
    logical: LogicalOperator,
    relational: RelationalOperator[T]
  )(implicit session: RelationalCypherSession[T]): RelationalCypherResult[T] =
    RelationalCypherResult(Some(logical), Some(relational))
}
