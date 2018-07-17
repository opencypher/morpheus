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

import org.apache.spark.sql.functions
import org.opencypher.okapi.api.schema._
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.impl.operators.{ExtractEntities, RelationalOperator, Start, TabularUnionAll}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable
import org.opencypher.spark.schema.CAPSSchema

class CAPSScanGraph(val scans: Seq[CAPSEntityTable], val schema: CAPSSchema, val tags: Set[Int])
  (implicit val session: CAPSSession)
  extends RelationalCypherGraph[DataFrameTable] {

  override type Records = CAPSRecords

  override type Session = CAPSSession

  private lazy val nodeTables = scans.collect { case it: CAPSNodeTable => it }

  private lazy val relTables = scans.collect { case it: CAPSRelationshipTable => it }

  // TODO: ScanGraph should be an operator that gets a set of tables as input
  private implicit def runtimeContext: RelationalRuntimeContext[DataFrameTable] = session.basicRuntimeContext()

  override def tables: Seq[DataFrameTable] = scans.map(_.table)

  // TODO: Use just one method for all entity scans
  override def nodes(name: String, nodeCypherType: CTNode, exactLabelMatch: Boolean = false): CAPSRecords = {
    val node = Var(name)(nodeCypherType)
    val selectedTables: Seq[CAPSNodeTable] = if (exactLabelMatch) {
      nodeTables.filter(_.entityType == nodeCypherType)
    } else {
      nodeTables.filter(_.entityType.subTypeOf(nodeCypherType).isTrue)
    }
    val schema = selectedTables.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetNodeHeader = schema.headerForNode(node)

    val nodeTablesAsStartOps: Seq[RelationalOperator[DataFrameTable]] = selectedTables.map(table => ExtractEntities(Start(table), targetNodeHeader, Set(node)))
    if (nodeTablesAsStartOps.isEmpty) {
      session.records.empty(targetNodeHeader)
    } else {
      val unionAllOp = nodeTablesAsStartOps.reduce(TabularUnionAll(_, _))
      session.records.from(unionAllOp.header, unionAllOp.table)
    }
  }

  // TODO: Use just one method for all entity scans
  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val rel = Var(name)(relCypherType)
    val scanTypes = relCypherType.types
    val selectedScans = relTables.filter(relTable => scanTypes.isEmpty || scanTypes.exists(relTable.entityType.types.contains))
    val schema = selectedScans.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetRelHeader = schema.headerForRelationship(rel)

    val scanRecords = selectedScans.map(_.records)

    // Filter rows that are relevant for the requested relationship type
    val filteredRecords = scanRecords
      .map { records =>
        val scanHeader = records.header
        val typeExprs = scanHeader
          .typesFor(Var("")(relCypherType))
          .filter(relType => relCypherType.types.contains(relType.relType.name))
          .toSeq

        typeExprs match {
          // no explicit type column
          case Nil =>
            records
          // multiple type columns
          case other =>
            val relTypeFilter = other
              .map(typeExpr => records.df.col(scanHeader.column(typeExpr)) === functions.lit(true))
              .reduce(_ || _)
            session.records.from(scanHeader, records.df.filter(relTypeFilter))
        }
      }

    val nodeTablesAsStartOps: Seq[RelationalOperator[DataFrameTable]] = filteredRecords.map(table => ExtractEntities(Start(table), targetRelHeader, Set(rel)))

    if (nodeTablesAsStartOps.isEmpty) {
      session.records.empty(targetRelHeader)
    } else {
      val unionAllOp = nodeTablesAsStartOps.reduce(TabularUnionAll(_, _))
      session.records.from(unionAllOp.header, unionAllOp.table)
    }
  }

  override def toString = s"CAPSScanGraph(${scans.map(_.entityType).mkString(", ")})"
}
