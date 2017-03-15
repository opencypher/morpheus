package org.opencypher.spark.prototype.impl.instances.spark

import org.apache.spark.sql.Column
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.record._
import org.opencypher.spark.prototype.api.spark.SparkCypherRecords
import org.opencypher.spark.prototype.impl.classy.Transform
import org.opencypher.spark.prototype.impl.syntax.header._
import org.opencypher.spark.prototype.impl.util.Replaced

trait SparkCypherRecordsInstances {

  implicit val sparkCypherRecordsTransform = new Transform[SparkCypherRecords] {
    override def filter(subject: SparkCypherRecords, expr: Expr): SparkCypherRecords = {
      val newData = subject.data.filter { row =>
        expr match {
          case Ands(exprs) =>
            exprs.foldLeft(true) {
              case (acc, subPredicate) =>
                val header = subject.header
                val slots = header.slotsFor(subPredicate)
                acc && row.getBoolean(slots.head.index)
            }
          case x =>
            throw new NotImplementedError(s"Can't filter using $x")
        }
      }

      new SparkCypherRecords {
        override def data = newData
        override def header = subject.header
      }
    }

    override def select(subject: SparkCypherRecords, fields: Map[Expr, String]): SparkCypherRecords = {

      val newHeader = subject.header.slots.foldLeft(RecordHeader.empty) {
        case (acc, RecordSlot(_, f: OpaqueField)) if fields.contains(f.key) => acc.update(addContent(f))._1
        case (acc, RecordSlot(_, f: ProjectedField)) if fields.contains(f.key) => acc.update(addContent(f))._1
        case (acc, _) => acc // drop projected exprs
      }

      val data = subject.data
      val columns = newHeader.slots.map { s =>
        val oldName = data.columns(subject.header.indexOf(s.content).get)
        val newName = fields(s.content.key)
        data.col(oldName).as(newName)
      }
      val newData = subject.data.select(columns: _*)

      new SparkCypherRecords {
        override def data = newData
        override def header = newHeader
      }
    }

    override def project(subject: SparkCypherRecords, it: ProjectedSlotContent): SparkCypherRecords = {

      val (newHeader, result) = subject.header.update(addContent(it))

      val newData = result match {
        case _: Replaced[_] =>
          subject.data  // no need to do more work
        case _ => ??? // need to evaluate the expression and construct new column
      }

      new SparkCypherRecords {
        override def data = newData
        override def header = newHeader
      }
    }

    override def join(lhs: SparkCypherRecords, rhs: SparkCypherRecords)(lhsSlot: RecordSlot, rhsSlot: RecordSlot): SparkCypherRecords = {
      val lhsData = lhs.data
      val rhsData = rhs.data

      val lhsColumn = lhsData.col(lhsData.columns(lhsSlot.index))
      val rhsColumn = rhsData.col(rhsData.columns(rhsSlot.index))

      val joinExpr: Column = lhsColumn === rhsColumn
      val joined = lhsData.join(rhsData, joinExpr, "inner")

      new SparkCypherRecords {
        override def data = joined
        override def header = lhs.header ++ rhs.header
      }
    }
  }
}
