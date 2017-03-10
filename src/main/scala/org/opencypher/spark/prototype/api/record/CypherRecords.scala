package org.opencypher.spark.prototype.api.record

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.prototype.api.expr.{Ands, Expr, HasLabel}

trait CypherRecords {
  type Data
  type Records <: CypherRecords

  def header: Seq[RecordSlot]
  def data: Data
}

trait SparkCypherRecords extends CypherRecords {
  self =>

  override type Data = DataFrame
  override type Records = SparkCypherRecords


  def filter(expr: Expr) = {
    val oldHeader = header
    val newData = data.filter { row =>
      expr match {
        case Ands(exprs) =>
          exprs.foldLeft(true) {
            case (acc, expr@HasLabel(node, label)) =>
              val optSlot = header.find { p =>
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
      override def header = oldHeader
    }
  }

  import org.apache.spark.sql.Column

  def select(fields: Map[Expr, String]) = {
    val columns = header.collect {
      case RecordSlot(name, expr, _) if fields.contains(expr) => name -> fields(expr)
    }.map {
      case (col, name) => new Column(col).as(name)
    }
    val newData = data.select(columns: _*)

    new SparkCypherRecords {
      override def data = newData

      override def header = self.header
    }
  }

  def toDF = data
}


