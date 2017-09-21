package org.opencypher.caps.api.spark

import org.apache.spark.sql.Row
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record.{OpaqueField, ProjectedExpr, RecordHeader, RecordSlot}
import org.opencypher.caps.api.types.CTNode
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.exception.Raise

import scala.collection.immutable

case class RowNodeExpansion(targetHeader: RecordHeader,
  targetNode: Var,
  nodesWithChildren: Map[Var, Seq[RecordSlot]],
  nodeColumnLookupTables: Map[Var, Map[String, String]])
  extends (Row => Seq[Row]) {

  val targetLabels = targetNode.cypherType match {
    case CTNode(labelMap) =>
      labelMap.collect {
        case (l, true) => l
      }.toSet
  }

  val labelColumnNamesLookupTable = nodesWithChildren.map { case (node, slots) =>
    val labelColumnNamesForNode = slots.collect {
      case RecordSlot(_, p@ProjectedExpr(HasLabel(_, l))) if targetLabels.contains(l.name) =>
        SparkColumnName.of(p)
    }
    node -> labelColumnNamesForNode
  }

  def apply(row: Row): Seq[Row] = {
    val adaptedRows = nodeColumnLookupTables.map { case (nodeVar, nodeLookupTable) =>
      val adaptedRow = adaptRowToNewHeader(row, nodeLookupTable)
      // TODO: Only return rows that have the target node labels
//      val labelIndexesForNode = labelColumnNamesLookupTable(nodeVar).map { labelColumnName =>
//        val columnIndex = adaptedRow.fieldIndex(labelColumnName)
//        columnIndex
//      }
//      val shouldReturn = labelIndexesForNode.forall(row.getBoolean(_))
//      if (shouldReturn) {
//        Some(adaptedRow)
//      } else {
//        None
//      }
      adaptedRow
    }
    adaptedRows.toSeq
  }


//  val rowsWithTargetLabel = adaptedRows.filter { row =>
//    val areAllLabelsSet = labelColumnNames.forall { labelColumnName =>
//      val i = row.fieldIndex(labelColumnName)
//      row.getBoolean(i)
//    }
//    areAllLabelsSet
//  }
//  rowsWithTargetLabel

  def adaptRowToNewHeader(row: Row, lookupTable: Map[String, String]): Row = {
    val orderedRowContent = targetHeader.slots.foldLeft(Seq.empty[Any]) { (newRowAcc, targetSlot) =>
      val maybeColumName = lookupTable.get(SparkColumnName.of(targetSlot))
      maybeColumName match {
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
