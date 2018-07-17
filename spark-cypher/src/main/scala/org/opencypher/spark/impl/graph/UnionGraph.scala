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
package org.opencypher.spark.impl.graph

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.impl.operators._
import org.opencypher.okapi.relational.impl.physical.RetagVariable
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable

// TODO: This should be a planned tree of physical operators instead of a graph
final case class UnionGraph(graphsToReplacements: Map[RelationalCypherGraph[DataFrameTable], Map[Int, Int]])(
  implicit override val session: CAPSSession,
  context: RelationalRuntimeContext[DataFrameTable]
) extends RelationalCypherGraph[DataFrameTable] {

  override type Records = CAPSRecords

  override type Session = CAPSSession

  require(graphsToReplacements.nonEmpty, "Union requires at least one graph")

  override def tables: Seq[DataFrameTable] = graphsToReplacements.keys.flatMap(_.tables).toSeq

  override lazy val tags: Set[Int] = graphsToReplacements.values.flatMap(_.values).toSet

  override lazy val schema: Schema = {
    graphsToReplacements.keys.map(g => g.schema).foldLeft(Schema.empty)(_ ++ _)
  }

  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean): CAPSRecords = {
    if (exactLabelMatch) {
      ToRefactor.nodesWithExactLabels(this, name, nodeCypherType.labels)
    } else {
      val node = Var(name)(nodeCypherType)
      val targetHeader = schema.headerForNode(node)
      val nodeOps: Iterable[RelationalOperator[DataFrameTable]] = graphsToReplacements.keys
        .filter(nodeCypherType.labels.isEmpty || _.schema.labels.intersect(nodeCypherType.labels).nonEmpty)
        .map {
          graph =>
            val nodeScan = graph.nodes(name, nodeCypherType)
            val startOp = Start(session.records.from(nodeScan.header, nodeScan.table))
            val retagOp = RetagVariable(startOp, node, graphsToReplacements(graph))
            ExtractEntities(retagOp, targetHeader, Set(node))
        }

      val distinctOp = Distinct(nodeOps.reduce(TabularUnionAll(_, _)), Set(node))

      session.records.from(distinctOp.header, distinctOp.table)
    }
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val rel = Var(name)(relCypherType)
    val targetHeader = schema.headerForRelationship(rel)
    val relOps: Iterable[RelationalOperator[DataFrameTable]] = graphsToReplacements.keys
      .filter(relCypherType.types.isEmpty || _.schema.relationshipTypes.intersect(relCypherType.types).nonEmpty)
      .map { graph =>
        val relScan = graph.relationships(name, relCypherType)
        val startOp = Start(session.records.from(relScan.header, relScan.table))
        val retagOp = RetagVariable(startOp, rel, graphsToReplacements(graph))
        ExtractEntities(retagOp, targetHeader, Set(rel))
      }

    val distinctOp = Distinct(relOps.reduce(TabularUnionAll(_, _)), Set(rel))

    session.records.from(distinctOp.header, distinctOp.table)
  }

  override def toString = s"CAPSUnionGraph(graphs=[${graphsToReplacements.mkString(",")}])"
}
