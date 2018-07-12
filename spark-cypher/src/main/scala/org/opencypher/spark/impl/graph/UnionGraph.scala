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
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.api.tagging.TagSupport.computeRetaggings
import org.opencypher.okapi.relational.impl.operators.{Distinct, ExtractEntities, Start, TabularUnionAll}

object UnionGraph {
  def apply[T <: FlatRelationalTable[T]](graphs: RelationalCypherGraph[T]*)(
    implicit session: RelationalCypherSession[T]
  ): UnionGraph[T] = {
    UnionGraph(computeRetaggings(graphs.map(g => g -> g.tags).toMap))
  }
}

// TODO: This should be a planned tree of physical operators instead of a graph
final case class UnionGraph[T <: FlatRelationalTable[T]](
  graphsToReplacements: Map[RelationalCypherGraph[T], Map[Int, Int]]
)
  (
    implicit val session: RelationalCypherSession[T],
    context: RelationalRuntimeContext[T]
  ) extends RelationalCypherGraph[T] {

  override type Graph = UnionGraph[T]

  require(graphsToReplacements.nonEmpty, "Union requires at least one graph")

  override lazy val tags: Set[Int] = graphsToReplacements.values.flatMap(_.values).toSet

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

    val distinctOp = Distinct(nodeOps.reduce(TabularUnionAll(_, _)), Set(node))

    session.records.from(distinctOp.header, distinctOp.table)
  }

  override def relationships(name: String, relCypherType: CTRelationship): RelationalCypherRecords[T] = {
    val rel = Var(name)(relCypherType)
    val targetHeader = schema.headerForRelationship(rel)
    val relOps = graphsToReplacements.keys
      .filter(relCypherType.types.isEmpty || _.schema.relationshipTypes.intersect(relCypherType.types).nonEmpty)
      .map { graph =>
        val relScan = graph.relationships(name, relCypherType)
        val replacements = graphsToReplacements(graph)

        val retaggedTable = relScan.header.idColumns(rel).foldLeft(relScan.table) {
          case (currentTable, idColumn) =>
            currentTable.retagColumn(replacements, idColumn)
        }
        ExtractEntities(Start(session.records.from(relScan.header, retaggedTable)), targetHeader, Set(rel))
      }

    val distinctOp = Distinct(relOps.reduce(TabularUnionAll(_, _)), Set(rel))

    session.records.from(distinctOp.header, distinctOp.table)
  }

  override def cache(): UnionGraph[T] = {
    graphsToReplacements.keys.foreach(_.cache())
    this
  }

  override def toString = s"CAPSUnionGraph(graphs=[${graphsToReplacements.mkString(",")}])"

  //  def nodesWithExactLabels(name: String, labels: Set[String]): RelationalCypherRecords[T] = {
  //    val nodeType = CTNode(labels)
  //    val nodeVar = Var(name)(nodeType)
  //    val records = nodes(name, nodeType)
  //
  //    val header = records.header
  //
  //    // compute slot contents to keep
  //    val labelExprs = header.labelsFor(nodeVar)
  //
  //    // need to iterate the slots to maintain the correct order
  //    val propertyExprs = schema.nodeKeys(labels).flatMap {
  //      case (key, cypherType) => Property(nodeVar, PropertyKey(key))(cypherType)
  //    }.toSet
  //    val headerPropertyExprs = header.propertiesFor(nodeVar).filter(propertyExprs.contains)
  //
  //    val keepExprs: Seq[Expr] = Seq(nodeVar) ++ labelExprs ++ headerPropertyExprs
  //
  //    val keepColumns = keepExprs.map(header.column)
  //
  //    // we only keep rows where all "other" labels are false
  //    val predicate = labelExprs
  //      .filterNot(l => labels.contains(l.label.name))
  //
  //      .map(header.column)
  //      .map(records.df.col(_) === false)
  //      .reduceOption(_ && _)
  //
  //    // filter rows and select only necessary columns
  //    val updatedData = predicate match {
  //
  //      case Some(filter) =>
  //        records.df
  //          .filter(filter)
  //          .select(keepColumns.head, keepColumns.tail: _*)
  //
  //      case None =>
  //        records.df.select(keepColumns.head, keepColumns.tail: _*)
  //    }
  //
  //    val updatedHeader = RecordHeader.from(keepExprs)
  //
  //    CAPSRecords(updatedHeader, updatedData)
  //  }
}
