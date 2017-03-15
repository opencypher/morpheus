package org.opencypher.spark.prototype.impl.instances.spark

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.opencypher.spark.api.types.{CTBoolean, CTFloat, CTInteger, CTString}
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.record._
import org.opencypher.spark.prototype.api.spark.SparkCypherRecords
import org.opencypher.spark.prototype.api.value.{CypherBoolean, CypherValue}
import org.opencypher.spark.prototype.impl.classy.Transform
import org.opencypher.spark.prototype.impl.physical.RuntimeContext
import org.opencypher.spark.prototype.impl.syntax.header._
import org.opencypher.spark.prototype.impl.util.{Found, Replaced}
import CypherValue.Conversion._

trait SparkCypherRecordsInstances extends Serializable {

  /*
   * Used when the predicate depends on values not stored inside the dataframe.
   */
  case class cypherFilter(header: RecordHeader, expr: Expr)
                         (implicit context: RuntimeContext) extends (Row => Boolean) {
    def apply(row: Row) = expr match {
      case Equals(p: Property, c: Const) =>
        val slot = header.slotsFor(p).head
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

  implicit val sparkCypherRecordsTransform = new Transform[SparkCypherRecords] with Serializable {

    override def filter(subject: SparkCypherRecords, expr: Expr)
                       (implicit context: RuntimeContext): SparkCypherRecords = {

      val newData = sqlFilter(subject.header, expr, subject.data) match {
        case Some(sqlExpr) =>
          subject.data.where(sqlExpr)
        case None =>
          subject.data.filter(cypherFilter(subject.header, expr))
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
        case _: Replaced[_] => subject.data
        case _: Found[_] => subject.data
        case x => // need to evaluate the expression and construct new column
          throw new NotImplementedError(s"Expected the slot to be replaced, but was $x")
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
