package org.opencypher.caps.api.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record.{ProjectedExpr, RecordHeader, RecordSlot}
import org.opencypher.caps.api.types.CTNode
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.exception.Raise

case class RowNodeExpansion(targetHeader: RecordHeader,
  targetNode: Var,
  nodesWithChildren: Map[Var, Seq[RecordSlot]],
  nodeColumnLookupTables: Map[Var, Map[String, String]])
  extends (Row => Seq[Row]) {

  private val targetLabels = targetNode.cypherType match {
    case CTNode(labelMap) =>
      labelMap.collect {
        case (l, true) => l
      }.toSet
  }

  private val rowSchema = StructType(targetHeader.slots.map(_.structField))

  private val labelIndexLookupTable = nodesWithChildren.map { case (node, slots) =>
    val labelIndicesForNode = slots.collect {
      case RecordSlot(_, p@ProjectedExpr(HasLabel(_, l))) if targetLabels.contains(l.name) =>
        rowSchema.fieldIndex(SparkColumnName.of(p.withOwner(targetNode)))
    }
    node -> labelIndicesForNode
  }

  def apply(row: Row): Seq[Row] = {
    val adaptedRows = nodeColumnLookupTables.flatMap { case (nodeVar, nodeLookupTable) =>
      val adaptedRow = adaptRowToNewHeader(row, nodeLookupTable)
      if (labelIndexLookupTable(nodeVar).forall(adaptedRow.getBoolean)) Some(adaptedRow)
      else None
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
