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

import org.apache.spark.sql.Row
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.Label
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.impl.convert.SparkConversions._

case class RowExpansion(
    targetHeader: RecordHeader,
    targetVar: Var,
    entitiesWithChildren: Map[Var, Set[Expr]],
    propertyColumnLookupTables: Map[Var, Map[String, String]]
) extends (Row => Seq[Row]) {

  private lazy val targetLabels = targetVar.cypherType match {
    case CTNode(labels, _) => labels
    case _ => Set.empty[String]
  }

  private val structType = targetHeader.toStructType

  private lazy val labelIndexLookupTable = entitiesWithChildren.map {
    case (node, exprs) =>
      val labelIndicesForNode = exprs.collect {
        case l@HasLabel(_, Label(name)) if targetLabels.contains(name) =>
          structType.fieldIndex(targetHeader.column(l.withOwner(targetVar)))
      }
      node -> labelIndicesForNode
  }

  private lazy val typeIndexLookupTable = entitiesWithChildren.map {
    case (rel, slots) =>
      val typeIndexForRel = slots.collectFirst {
        case h@HasType(r, _) if r == rel =>
          structType.fieldIndex(targetHeader.column(h.withOwner(targetVar)))
      }.getOrElse(throw IllegalArgumentException(s"a type column for relationship $rel"))
      rel -> typeIndexForRel
  }

  def apply(row: Row): Seq[Row] = {
    val adaptedRows = propertyColumnLookupTables.flatMap {
      case (entity, nodeLookupTable) =>
        val adaptedRow = adaptRowToNewHeader(row, nodeLookupTable)
        targetVar.cypherType match {
          case _: CTNode =>
            val indices = labelIndexLookupTable(entity)
            val hasAllRequiredLabels = indices.forall(adaptedRow.getBoolean)
            if (hasAllRequiredLabels) Some(adaptedRow)
            else None
          case CTRelationship(types, _) if types.isEmpty =>
            Some(adaptedRow)
          case CTRelationship(types, _) =>
            val index = typeIndexLookupTable(entity)
            val relType = adaptedRow.getString(index)
            val hasMatchingType = types.contains(relType)
            if (hasMatchingType) Some(adaptedRow)
            else None
          case _ =>
            throw IllegalArgumentException("an entity variable", entity)
        }
    }

    filterNullRows(adaptedRows.toSeq)
  }

  def adaptRowToNewHeader(row: Row, lookupTable: Map[String, String]): Row = {
    val orderedRowContent = targetHeader.columns.toSeq.sorted.foldLeft(Seq.empty[Any]) { (currentRow, column) =>
      val maybeColumnName = lookupTable.get(column)
      maybeColumnName match {
        case Some(columnName) =>
          val index = row.fieldIndex(columnName)
          currentRow :+ row.get(index)
        case None =>
          // TODO: this is wrong but might usually work
          val value = targetHeader.expressionsFor(column).head match {
            case HasLabel(_, _) => false
            case Property(_, _) => null
            case other => throw IllegalArgumentException("a projected expression of label or property", other)
          }
          currentRow :+ value
      }
    }
    Row.fromSeq(orderedRowContent)
  }

  def filterNullRows(rows: Seq[Row]): Seq[Row] = {
    val entityVar = targetHeader.entityVars.head
    val index = structType.fieldIndex(targetHeader.column(entityVar))
    rows.filterNot(_.isNullAt(index))
  }
}
