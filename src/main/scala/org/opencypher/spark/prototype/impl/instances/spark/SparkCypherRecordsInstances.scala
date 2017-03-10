package org.opencypher.spark.prototype.impl.instances.spark

import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.record._
import org.opencypher.spark.prototype.api.spark.SparkCypherRecords
import org.opencypher.spark.prototype.impl.classy.Transform

trait SparkCypherRecordsInstances {

  implicit val sparkCypherRecordsTransform = new Transform[SparkCypherRecords] {
    override def filter(subject: SparkCypherRecords, expr: Expr): SparkCypherRecords = {
      val newData = subject.data.filter { row =>
        expr match {
          case Ands(exprs) =>
            exprs.foldLeft(true) {
              case (acc, expr@HasLabel(node, label)) =>
                val header = subject.header
                val optSlot: Option[RecordSlot] = ???
                println(subject.data.schema)
                acc && optSlot.exists(slot => row.getBoolean(row.fieldIndex(??? /* header.column(slot) */)))
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
//      val columns = subject.header.slots.collect {
//        case r@RecordSlot(ExprSlotKey(expr), _) if fields.contains(expr) => r.toString -> fields(expr)
//        case RecordSlot(FieldSlotKey(field, expr), _) => field -> fields(expr) // TODO: Really?
//      }.map {
//        case (col, name) => new Column(col).as(name)
//      }
//      val newData = subject.data.select(columns: _*)

      new SparkCypherRecords {
        override def data = ???
        override def header = subject.header
      }
    }

    override def project(subject: SparkCypherRecords, it: ProjectedSlotContent): SparkCypherRecords = {

      val expr = it.expr
      val field = it.alias.get

//      val newHeader = subject.header.slotsByExpr(expr).headOption match {
//        case None => ??? // new expression
//        case Some(slot) =>
//          val newExpr = RecordSlot(FieldSlotKey(field, expr), slot.cypherType)
//          val newVar = RecordSlot(FieldSlotKey(field, Var(field)), slot.cypherType)
//          subject.header + newExpr + newVar
//      }

      new SparkCypherRecords {
        override def data = subject.data
        override def header = ??? // newHeader
      }
    }
  }
}
