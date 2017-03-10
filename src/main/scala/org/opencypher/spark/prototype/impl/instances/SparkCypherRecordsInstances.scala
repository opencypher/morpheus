package org.opencypher.spark.prototype.impl.instances

import org.apache.spark.sql.Column
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.record.{RecordSlot, SparkCypherRecords}
import org.opencypher.spark.prototype.impl.classy.Transform

class SparkCypherRecordsInstances {

  implicit val sparkCypherRecordsTransform = new Transform[SparkCypherRecords] {
    override def filter(subject: SparkCypherRecords, expr: Expr): SparkCypherRecords = {
      val newData = subject.data.filter { row =>
        expr match {
          case Ands(exprs) =>
            exprs.foldLeft(true) {
              case (acc, expr@HasLabel(node, label)) =>
                val optSlot = subject.header.find { p =>
                  p.expr == expr
                }
                acc && optSlot.exists(slot => row.getBoolean(row.fieldIndex(slot.name)))
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
      val columns = subject.header.collect {
        case RecordSlot(name, expr, _) if fields.contains(expr) => name -> fields(expr)
      }.map {
        case (col, name) => new Column(col).as(name)
      }
      val newData = subject.data.select(columns: _*)

      new SparkCypherRecords {
        override def data = newData
        override def header = subject.header
      }
    }

    def exprToVariable(expr: Expr, globalsRegistry: GlobalsRegistry) = expr match {
      case v: Var => v
      case Property(v: Var, ref) => Var(v.name + "." + globalsRegistry.propertyKey(ref).name)
      case _ => ???
    }

    override def project(subject: SparkCypherRecords, expr: Expr, globalsRegistry: GlobalsRegistry): SparkCypherRecords = {

      val newHeader = subject.header.find(_.expr == expr) match {
        case None => ??? // new expression
        case Some(slot) => subject.header :+ RecordSlot(slot.name, exprToVariable(expr, globalsRegistry), slot.cypherType)
      }

      new SparkCypherRecords {
        override def data = subject.data
        override def header = newHeader
      }
    }
  }
}
