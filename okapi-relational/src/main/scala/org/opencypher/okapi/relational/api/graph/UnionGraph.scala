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
package org.opencypher.okapi.relational.api.graph

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.api.tagging.TagSupport.computeRetaggings
import org.opencypher.okapi.relational.impl.operators.{Distinct, ExtractEntities, Start}

object UnionGraph {
  def apply[T <: FlatRelationalTable[T]](
    graphs: RelationalCypherGraph[T]*
  )(implicit session: RelationalCypherSession[T]): UnionGraph[T] = {
    UnionGraph(computeRetaggings(graphs.map(g => g -> g.tags).toMap))
  }
}

// TODO: This should be a planned tree of physical operators instead of a graph
final case class UnionGraph[T <: FlatRelationalTable[T]](
  graphsToReplacements: Map[RelationalCypherGraph[T], Map[Int, Int]]
)(implicit val session: RelationalCypherSession[T], context: RelationalRuntimeContext[T]) extends RelationalCypherGraph[T] {

  require(graphsToReplacements.nonEmpty, "Union requires at least one graph")

  override lazy val tags: Set[Int] = graphsToReplacements.values.flatMap(_.values).toSet

  override def toString = s"CAPSUnionGraph(graphs=[${graphsToReplacements.mkString(",")}])"

  override lazy val schema: Schema = {
    graphsToReplacements.keys.map(g => g.schema).foldLeft(Schema.empty)(_ ++ _)
  }

  override def nodes(name: String, nodeCypherType: CTNode): RelationalCypherRecords[T] = {
    val node = Var(name)(nodeCypherType)
    val targetHeader = schema.headerForNode(node)
    val nodeOps = graphsToReplacements.keys
      .filter(nodeCypherType.labels.isEmpty || _.schema.labels.intersect(nodeCypherType.labels).nonEmpty)
      .map {
        graph =>
          val nodeScan = graph.nodes(name, nodeCypherType)
          val replacements = graphsToReplacements(graph)

          val retaggedTable = nodeScan.header.idColumns(node).foldLeft(nodeScan.table) {
            case (currentTable, idColumn) =>
              currentTable.retagColumn(replacements, idColumn)
          }
          ExtractEntities(Start(session.records.from(nodeScan.header, retaggedTable)), targetHeader, Set(node))
      }

    val nodeTables = nodeOps.map(_.table)
    val unionAllTable: T = if (nodeTables.isEmpty) {
      session.records.empty(targetHeader)
    } else {
      nodeTables.reduce(_ unionAll _)
    }

    val distinctOp = Distinct(Start.from(targetHeader, unionAllTable), Set(node))

    session.records.from(distinctOp.header, distinctOp.table)
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val rel = Var(name)(relCypherType)
    val targetHeader = schema.headerForRelationship(rel)
    val relScans = graphsToReplacements.keys
      .filter(relCypherType.types.isEmpty || _.schema.relationshipTypes.intersect(relCypherType.types).nonEmpty)
      .map { graph =>
        val relScan = graph.relationships(name, relCypherType)
        val replacements = graphsToReplacements(graph)

        val retaggedTable = relScan.header.idColumns(rel).foldLeft(relScan.table) {
          case (currentTable, idColumn) =>
            currentTable.retagColumn(replacements, idColumn)
        }
        relScan.from(relScan.header, retaggedTable)
      }

    alignRecords(relScans.toSeq, rel, targetHeader)
      .map(_.distinct(rel))
      .getOrElse(CAPSRecords.empty(targetHeader))
  }
}
