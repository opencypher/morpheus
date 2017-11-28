/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.caps.impl.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record.{ProjectedExpr, RecordHeader, RecordSlot}
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.impl.record.CAPSRecordHeader._
import org.opencypher.caps.impl.exception.Raise

case class RowExpansion(
  targetHeader: RecordHeader,
  targetVar: Var,
  entitiesWithChildren: Map[Var, Seq[RecordSlot]],
  propertyColumnLookupTables: Map[Var, Map[String, String]]
) extends (Row => Seq[Row]) {

  private lazy val targetLabels = targetVar.cypherType match {
    case CTNode(labels) => labels
    case _ => Set.empty[String]
  }

  private val rowSchema = StructType(targetHeader.slots.map(_.asStructField))

  private lazy val labelIndexLookupTable = entitiesWithChildren.map { case (node, slots) =>
    val labelIndicesForNode = slots.collect {
      case RecordSlot(_, p@ProjectedExpr(HasLabel(_, l))) if targetLabels.contains(l.name) =>
        rowSchema.fieldIndex(SparkColumnName.of(p.withOwner(targetVar)))
    }
    node -> labelIndicesForNode
  }

  private lazy val typeIndexLookupTable = entitiesWithChildren.map { case (rel, slots) =>
    val typeIndexForRel = slots.collectFirst {
      case RecordSlot(_, p@ProjectedExpr(OfType(r))) if r == rel =>
        rowSchema.fieldIndex(SparkColumnName.of(p.withOwner(targetVar)))
    }.getOrElse(Raise.impossible("relationship didn't have a type column!"))
    rel -> typeIndexForRel
  }

  def apply(row: Row): Seq[Row] = {
    val adaptedRows = propertyColumnLookupTables.flatMap { case (entity, nodeLookupTable) =>
      val adaptedRow = adaptRowToNewHeader(row, nodeLookupTable)
      targetVar.cypherType match {
        case _: CTNode =>
          val indices = labelIndexLookupTable(entity)
          val hasAllRequiredLabels = indices.forall(adaptedRow.getBoolean)
          if (hasAllRequiredLabels) Some(adaptedRow)
          else None
        case CTRelationship(types) if types.isEmpty =>
          Some(adaptedRow)
        case CTRelationship(types) =>
          val index = typeIndexLookupTable(entity)
          val relType = adaptedRow.getString(index)
          val hasMatchingType = types.contains(relType)
          if (hasMatchingType) Some(adaptedRow)
          else None
        case _ =>
          Raise.invalidArgument("an entity variable", entity)
      }
    }
    adaptedRows.toSeq
  }

  def adaptRowToNewHeader(row: Row, lookupTable: Map[String, String]): Row = {
    val orderedRowContent = targetHeader.slots.foldLeft(Seq.empty[Any]) { (newRowAcc, targetSlot) =>
      val maybeColumnName = lookupTable.get(SparkColumnName.of(targetSlot))
      maybeColumnName match {
        case Some(columnName) =>
          val index = row.fieldIndex(columnName)
          newRowAcc :+ row.get(index)
        case None =>
          val value = targetSlot.content match {
            case ProjectedExpr(HasLabel(_, _)) => false
            case ProjectedExpr(Property(_, _)) => null
            case _ => Raise.impossible()
          }
          newRowAcc :+ value
      }
    }
    Row.fromSeq(orderedRowContent)
  }
}
