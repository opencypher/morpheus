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
package org.opencypher.spark.impl

import org.apache.spark.sql.functions
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.schema._
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.schema.CAPSSchema

class CAPSScanGraph(val scans: Seq[CAPSEntityTable], val schema: CAPSSchema, val tags: Set[Int])
  (implicit val session: CAPSSession)
  extends CAPSGraph {

  // TODO: Normalize (remove redundant columns for implied Schema information, clear aliases?)

  self: CAPSGraph =>

  override def toString = s"CAPSScanGraph(${scans.map(_.entityType).mkString(", ")})"

  private lazy val nodeTables = scans.collect { case it: CAPSNodeTable => it }

  private lazy val relTables = scans.collect { case it: CAPSRelationshipTable => it }

  override def cache(): CAPSScanGraph = forEach(_.table.cache())

  override def persist(): CAPSScanGraph = forEach(_.table.persist())

  private def forEach(f: CAPSEntityTable => Unit): CAPSScanGraph = {
    scans.foreach(f)
    this
  }

  override def persist(storageLevel: StorageLevel): CAPSScanGraph = forEach(_.table.persist(storageLevel))

  override def unpersist(): CAPSScanGraph = forEach(_.table.unpersist())

  override def unpersist(blocking: Boolean): CAPSScanGraph = forEach(_.table.unpersist(blocking))

  override def nodes(name: String, nodeCypherType: CypherType): CAPSRecords =
    nodesInternal(name, nodeCypherType, byExactType = false)

  override def nodesWithExactLabels(name: String, labels: Set[String]): CAPSRecords =
    nodesInternal(name, CTNode(labels), byExactType = true)

  private def nodesInternal(name: String, nodeCypherType: CypherType, byExactType: Boolean): CAPSRecords = {
    val node = Var(name)(nodeCypherType)
    val selectedTables = if (byExactType) {
      nodeTables.filter(_.entityType == nodeCypherType)
    } else {
      nodeTables.filter(_.entityType.subTypeOf(nodeCypherType))
    }
    val schema = selectedTables.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetNodeHeader = schema.headerForNode(node)

    alignRecords(selectedTables.map(_.records), node, targetNodeHeader).getOrElse(CAPSRecords.empty(targetNodeHeader))
  }

  override def relationships(name: String, relCypherType: CypherType): CAPSRecords = {
    val rel = Var(name)(relCypherType)
    val scanTypes = relCypherType.relTypes
    val selectedScans = relTables.filter(relTable => scanTypes.isEmpty || scanTypes.exists(relTable.entityType.relTypes.contains))
    val schema = selectedScans.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetRelHeader = schema.headerForRelationship(rel)

    val scanRecords = selectedScans.map(_.records)

    // Filter rows that are relevant for the requested relationship type
    val filteredRecords = scanRecords
      .map { records =>
        val scanHeader = records.header
        val typeExprs = scanHeader
          .typesFor(Var("")(relCypherType))
          .filter(relExpr => scanTypes.contains(relExpr.relType.name))
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
            CAPSRecords(scanHeader, records.df.filter(relTypeFilter))
        }
      }

    alignRecords(filteredRecords, rel, targetRelHeader).getOrElse(CAPSRecords.empty(targetRelHeader))
  }
}
