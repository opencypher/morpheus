package org.opencypher.caps.api.spark

import org.apache.spark.sql.Row
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record.{OpaqueField, ProjectedExpr, RecordHeader}
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.exception.Raise

case class RowNodeExpansion(targetHeader: RecordHeader,
                              nodeColumnLookupTables: Map[Var,Map[String, String]])
    extends (Row => Vector[Row]) {

    def apply(row: Row): Vector[Row] = {
      nodeColumnLookupTables.map { case (nodeVar, nodeLookupTable) =>
        adaptRowToNewHeader(row, targetHeader, nodeLookupTable)
      }.toVector
    }

    def adaptRowToNewHeader(row: Row, targetHeader: RecordHeader, lookupTable: Map[String, String]): Row = {
      val orderedRowContent = targetHeader.slots.foldLeft(Seq.empty[Any]) { (newRowAcc, targetSlot) =>
        val maybeColumName = lookupTable.get(SparkColumnName.of(targetSlot))
        maybeColumName match {
          case Some(columnName) =>
            val index = row.fieldIndex(columnName)
            newRowAcc :+ row.get(index)
          case None =>
            val value = targetSlot.content match {
              case ProjectedExpr(HasLabel(_,_)) => false
              case ProjectedExpr(Property(_,_)) => null
              case _ => Raise.impossible()
            }
            newRowAcc :+ value
        }
      }
      Row.fromSeq(orderedRowContent)
    }
  }
