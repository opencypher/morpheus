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
package org.opencypher.okapi.relational.impl.graph

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}
import org.opencypher.okapi.relational.impl.operators.{Distinct, RelationalOperator, TabularUnionAll}
import org.opencypher.okapi.relational.impl.planning.RelationalPlanner._

// TODO: This should be a planned tree of physical operators instead of a graph
final case class UnionGraph[T <: Table[T]](graphsToReplacements: Map[RelationalCypherGraph[T], Map[Int, Int]])
  (implicit context: RelationalRuntimeContext[T]) extends RelationalCypherGraph[T] {

  override implicit val session: RelationalCypherSession[T] = context.session

  override type Records = RelationalCypherRecords[T]

  override type Session = RelationalCypherSession[T]

  require(graphsToReplacements.nonEmpty, "Union requires at least one graph")

  override def tables: Seq[T] = graphsToReplacements.keys.flatMap(_.tables).toSeq

  override lazy val tags: Set[Int] = graphsToReplacements.values.flatMap(_.values).toSet

  override lazy val schema: Schema = {
    graphsToReplacements.keys.map(g => g.schema).foldLeft(Schema.empty)(_ ++ _)
  }

  override def toString = s"UnionGraph(graphs=[${graphsToReplacements.mkString(",")}])"

  protected override def scanOperator(
    entityType: CypherType,
    exactLabelMatch: Boolean
  ): RelationalOperator[T] = {
    val targetEntity = Var("")(entityType)
    val targetEntityHeader = schema.headerForEntity(targetEntity, exactLabelMatch)
    val alignedScans = graphsToReplacements.keys
      .map { graph =>
        val scanOp = graph.scanOperator(entityType, exactLabelMatch)
        val retagOp = scanOp.retagVariable(targetEntity, graphsToReplacements(graph))
        retagOp.alignWith(targetEntity, targetEntityHeader)
      }
    // TODO: find out if a graph returns empty records and skip union operation
    Distinct(alignedScans.reduce(TabularUnionAll(_, _)), Set(targetEntity))
  }
}
