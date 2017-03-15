package org.opencypher.spark.prototype.impl.instances.spark

import org.apache.spark.sql.{Column, Row}
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

  case class equality(row: Row, rhs: CypherValue, slot: RecordSlot) {
    def apply(): Boolean = {

      // TODO: Could also use an Any => CypherValue conversion -- not sure which is better
      slot.content.cypherType.material match {
        case CTBoolean => cypherBoolean(row.getBoolean(slot.index)) == rhs
        case CTString => cypherString(row.getString(slot.index)) == rhs
        case CTInteger => cypherInteger(row.getLong(slot.index)) == rhs
        case CTFloat => cypherFloat(row.getDouble(slot.index)) == rhs
        case x => throw new NotImplementedError(
          s"Can not compare values of type $x yet (attempted ${row.get(slot.index)} = $rhs")
      }
    }
  }

  case class filtering(subject: SparkCypherRecords, expr: Expr, context: RuntimeContext) extends (Row => Boolean) {
    def apply(row: Row): Boolean = expr match {
      case Ands(exprs) =>
        exprs.foldLeft(true) {
          case (acc, HasType(rel, ref)) =>
            val idSlot = subject.header.typeId(rel)
            acc && row.getInt(idSlot.index) == ref.id
          case (acc, subPredicate) =>
            val header = subject.header
            val slots = header.slotsFor(subPredicate)
            acc && row.getBoolean(slots.head.index)
        }
      case Equals(p: Property, c: Const) =>
        val slot = subject.header.slotsFor(p).head
        val rhs = context.constants(c.ref)
        equality(row, rhs, slot)()
      case x =>
        throw new NotImplementedError(s"Can't filter using $x")
    }
  }

  implicit val sparkCypherRecordsTransform = new Transform[SparkCypherRecords] with Serializable {

    override def filter(subject: SparkCypherRecords, expr: Expr)
                       (implicit context: RuntimeContext): SparkCypherRecords = {
      // TODO: Construct Spark SQL expression for filters that may be done on columns



      val newData = subject.data.filter(filtering(subject, expr, context))

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
