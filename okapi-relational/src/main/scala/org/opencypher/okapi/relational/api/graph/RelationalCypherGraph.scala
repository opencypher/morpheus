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
package org.opencypher.okapi.relational.api.graph

import org.opencypher.okapi.api.graph.{PropertyGraph, QualifiedGraphName, _}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix
import org.opencypher.okapi.ir.api.expr.{NodeVar, RelationshipVar}
import org.opencypher.okapi.ir.impl.util.VarConverters._
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.planning.{RelationalCypherResult, RelationalRuntimeContext}
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}
import org.opencypher.okapi.relational.impl.graph.{EmptyGraph, PrefixedGraph, ScanGraph, UnionGraph}
import org.opencypher.okapi.relational.impl.operators.{AlignColumnsWithReturnItems, RelationalOperator, Select}
import org.opencypher.okapi.relational.impl.planning.RelationalPlanner._

import scala.reflect.runtime.universe.TypeTag

trait RelationalCypherGraphFactory[T <: Table[T]] {

  type Graph = RelationalCypherGraph[T]

  implicit val session: RelationalCypherSession[T]

  private[opencypher] implicit def tableTypeTag: TypeTag[T] = session.tableTypeTag

  def prefixedGraph(graph: RelationalCypherGraph[T], prefix: GraphIdPrefix)(implicit context: RelationalRuntimeContext[T]): Graph =
    PrefixedGraph(graph, prefix)

  def unionGraph(graphs: RelationalCypherGraph[T]*)(implicit context: RelationalRuntimeContext[T]): Graph =
    UnionGraph(graphs.toList)

  def empty: Graph = EmptyGraph()

  def create(nodeTable: EntityTable[T], entityTables: EntityTable[T]*): Graph = {
    create(None, nodeTable +: entityTables: _*)
  }

  def create(maybeSchema: Option[Schema], nodeTable: EntityTable[T], entityTables: EntityTable[T]*): Graph = {
    create(maybeSchema, nodeTable +: entityTables: _*)
  }

  def create(
    maybeSchema: Option[Schema],
    entityTables: EntityTable[T]*
  ): Graph = {
    implicit val runtimeContext: RelationalRuntimeContext[T] = session.basicRuntimeContext()
    val allTables = entityTables
    val schema = maybeSchema.getOrElse(allTables.map(_.schema).reduce[Schema](_ ++ _))
    new ScanGraph(allTables, schema)
  }
}

trait RelationalCypherGraph[T <: Table[T]] extends PropertyGraph {

  type Records <: RelationalCypherRecords[T]

  type Session <: RelationalCypherSession[T]

  override def session: Session

  private[opencypher] implicit def tableTypeTag: TypeTag[T] = session.tableTypeTag

  def cache(): RelationalCypherGraph[T] = {
    tables.foreach(_.cache())
    this
  }

  def tables: Seq[T]

  def scanOperator(searchPattern: Pattern, exactLabelMatch: Boolean = false): RelationalOperator[T]

  override def cypher(
    query: String,
    parameters: CypherValue.CypherMap,
    drivingTable: Option[CypherRecords],
    queryCatalog: Map[QualifiedGraphName, PropertyGraph]
  ): RelationalCypherResult[T] = session.cypherOnGraph(this, query, parameters, drivingTable, queryCatalog)

  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean = false): RelationalCypherRecords[T] = {
    val pattern = NodePattern(nodeCypherType)
    val scan = scanOperator(pattern, exactLabelMatch)
    val nodeVar = NodeVar(name)(nodeCypherType)
    val namedScan = AlignColumnsWithReturnItems(Select(scan.assignScanName(Map(pattern.nodeEntity.toVar -> nodeVar)), List(nodeVar)))
    session.records.from(namedScan.header, namedScan.table)
  }

  override def relationships(name: String, relCypherType: CTRelationship): RelationalCypherRecords[T] = {
    val pattern = RelationshipPattern(relCypherType)
    val scan = scanOperator(pattern)
    val relVar = RelationshipVar(name)(relCypherType)
    val namedScan = AlignColumnsWithReturnItems(Select(scan.assignScanName(Map(pattern.relEntity.toVar -> relVar)), List(relVar)))
    session.records.from(namedScan.header, namedScan.table)
  }

  override def unionAll(others: PropertyGraph*): RelationalCypherGraph[T] = {
    val otherGraphs: List[RelationalCypherGraph[T]] = others.toList.map {
      case g: RelationalCypherGraph[T] => g
      case _ => throw UnsupportedOperationException("Union all only works on relational graphs")
    }

    // TODO: parameterize property graph API with actual graph type to allow for type safe implementations!
    val graphAt = (qgn: QualifiedGraphName) => Some(session.catalog.graph(qgn) match {
      case g: RelationalCypherGraph[_] => g.asInstanceOf[RelationalCypherGraph[T]]
    })

    implicit val context: RelationalRuntimeContext[T] = RelationalRuntimeContext(graphAt)(session)

    val allGraphs = (this :: otherGraphs).zipWithIndex.map { case (g, i) => session.graphs.prefixedGraph(g, i.toByte) }
    session.graphs.unionGraph(allGraphs: _*)
  }
}
