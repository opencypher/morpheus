package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.spark.SparkCypherRecords
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.value.CypherValue.Conversion._
import org.opencypher.spark.impl.classes.Transform
import org.opencypher.spark.impl.physical.RuntimeContext
import org.opencypher.spark.impl.syntax.header._
import org.opencypher.spark.impl.util.{Found, Replaced}

trait SparkCypherRecordsInstances extends Serializable {

  /*
   * Used when the predicate depends on values not stored inside the dataframe.
   */
  case class cypherFilter(header: RecordHeader, expr: Expr)
                         (implicit context: RuntimeContext) extends (Row => Boolean) {
    def apply(row: Row) = expr match {
      case Equals(p: Property, c: Const) =>
        val slot = header.slotsFor(p).headOption match {
          case Some(s) => s
          case None => throw new IllegalStateException(s"Expected to find $p in $header")
        }
        val rhs = context.constants(c.ref)

        // TODO: Could also use an Any => CypherValue conversion -- not sure which is better
        slot.content.cypherType.material match {
          case CTBoolean => cypherBoolean(row.getBoolean(slot.index)) == rhs
          case CTString => cypherString(row.getString(slot.index)) == rhs
          case CTInteger => cypherInteger(row.getLong(slot.index)) == rhs
          case CTFloat => cypherFloat(row.getDouble(slot.index)) == rhs
          case x => throw new NotImplementedError(
            s"Can not compare values of type $x yet (attempted ${row.get(slot.index)} = $rhs")
        }
      case x =>
        throw new NotImplementedError(s"Predicate $x not yet supported")
    }
  }

  /*
   * Attempts to create SparkSQL expression for use when filtering
   */
  def sqlFilter(header: RecordHeader, expr: Expr, df: DataFrame): Option[Column] = {
    expr match {
      case Not(Equals(v1: Var, v2: Var)) =>
        val lhsSlot = header.slotFor(v1)
        val rhsSlot = header.slotFor(v2)
        Some(new Column(df.columns(lhsSlot.index)) =!= new Column(df.columns(rhsSlot.index)))
      case Ands(exprs) =>
        val cols = exprs.map(sqlFilter(header, _, df))
        if (cols.contains(None)) None
        else {
          cols.reduce[Option[Column]] {
            case (Some(l: Column), Some(r: Column)) => Some(l && r)
            case _ => throw new IllegalStateException("This should never happen")
          }
        }
      case HasType(rel, ref) =>
        val idSlot = header.typeId(rel)
        Some(new Column(df.columns(idSlot.index)) === ref.id)
      case h: HasLabel =>
        val slot = header.slotsFor(h).head
        Some(new Column(df.columns(slot.index))) // it's a boolean column
      case _ => None
    }
  }

  implicit def sparkCypherRecordsTransform(implicit context: RuntimeContext) = new Transform[SparkCypherRecords] with Serializable {

    override def filter(subject: SparkCypherRecords, expr: Expr, nextHeader: RecordHeader): SparkCypherRecords = {

      val filteredRows = sqlFilter(subject.header, expr, subject.data) match {
        case Some(sqlExpr) =>
          subject.data.where(sqlExpr)
        case None =>
          subject.data.filter(cypherFilter(nextHeader, expr))
      }

      val selectedColumns = nextHeader.slots.map { c =>
        val name = context.columnName(c)
        filteredRows.col(name)
      }

      val nextData = filteredRows.select(selectedColumns: _*)

      new SparkCypherRecords {
        override def data = nextData
        override def header = nextHeader
      }
    }

    // TODO: Correctly handle aliasing in the header
    override def select(subject: SparkCypherRecords, fields: IndexedSeq[Var], nextHeader: RecordHeader): SparkCypherRecords = {

      val data = subject.data
      val columns = fields.map { f =>
        data.col(data.columns(subject.header.slotsFor(f).head.index))
      }
      val newData = subject.data.select(columns: _*)

      new SparkCypherRecords {
        override def data = newData
        override def header = nextHeader
      }
    }

    override def reorder(subject: SparkCypherRecords, nextHeader: RecordHeader): SparkCypherRecords = {
      val columns = nextHeader.slots.map(context.columnName)

      val newData = subject.data.select(columns.head, columns.tail: _*)

      new SparkCypherRecords {
        override def data = newData
        override def header = nextHeader
      }
    }

    override def alias2(subject: SparkCypherRecords, expr: Expr, v: Var, nextHeader: RecordHeader): SparkCypherRecords = {
      val oldSlot = subject.header.slotsFor(expr).head

      val newSlot = nextHeader.slotsFor(v).head

      val oldColumnName = context.columnName(oldSlot)
      val newColumnName = context.columnName(newSlot)

      val nextData = if (subject.data.columns.contains(oldColumnName)) {
        subject.data.withColumnRenamed(oldColumnName, newColumnName)
      } else {
        throw new IllegalStateException(s"Wanted to rename column $oldColumnName, but it was not present!")
      }

      new SparkCypherRecords {
        override def data = nextData
        override def header = nextHeader
      }
    }

    override def project(subject: SparkCypherRecords, expr: Expr, nextHeader: RecordHeader): SparkCypherRecords = {

      val slots = nextHeader.slotsFor(expr)

      if (slots.isEmpty) {
        throw new IllegalStateException(s"No slot found for expression $expr")
      } else if (slots.size > 1) {
        throw new NotImplementedError("No support for multi-column expressions yet")
      } else {
        val slot = slots.head

        val newData = expr match {
          case Subtract(lhs, rhs) =>
            val lhsSlot = nextHeader.slotsFor(lhs).head
            val rhsSlot = nextHeader.slotsFor(rhs).head

            val columns = subject.data.columns
            val lhsColumn = subject.data.col(columns(lhsSlot.index))
            val rhsColumn = subject.data.col(columns(rhsSlot.index))

            val columnsToSelect = columns.map(subject.data.col) :+ (lhsColumn - rhsColumn).as(context.columnName(slot))
            subject.data.select(columnsToSelect: _*)
          case _ => throw new NotImplementedError(s"No support for $expr yet")
        }

        new SparkCypherRecords {
          override def data = newData
          override def header = nextHeader
        }
      }
    }

    override def join(lhs: SparkCypherRecords, rhs: SparkCypherRecords)
                     (lhsSlot: RecordSlot, rhsSlot: RecordSlot): SparkCypherRecords =
      join(lhs, rhs, lhs.header ++ rhs.header)(lhsSlot, rhsSlot)

    override def join(lhs: SparkCypherRecords, rhs: SparkCypherRecords, nextHeader: RecordHeader)
                     (lhsSlot: RecordSlot, rhsSlot: RecordSlot): SparkCypherRecords = {
      val lhsData = lhs.data
      val rhsData = rhs.data

      val lhsColumn = lhsData.col(lhsData.columns(lhsSlot.index))
      val rhsColumn = rhsData.col(rhsData.columns(rhsSlot.index))

      val joinExpr: Column = lhsColumn === rhsColumn
      val joined = lhsData.join(rhsData, joinExpr, "inner")

      new SparkCypherRecords {
        override def data = joined
        override def header = nextHeader
      }
    }
  }
}
