package org.opencypher.spark.prototype.impl.instances.spark

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
        val oldName = data.columns(s.index)
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
  }
}
