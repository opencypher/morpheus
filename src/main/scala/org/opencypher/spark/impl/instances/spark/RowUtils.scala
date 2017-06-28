package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.Row
import org.opencypher.spark.api.expr.{Const, Expr}
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.api.value.CypherValue.Conversion._
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.physical.RuntimeContext

object RowUtils {

  implicit class CypherRow(r: Row) {
    def getCypherValue(expr: Expr, header: RecordHeader)(implicit context: RuntimeContext): CypherValue = {
      expr match {
        case c: Const => context.parameters(context.constants.constantRef(c.constant))
        case _ =>
          header.slotsFor(expr).headOption match {
            case None => Raise.slotNotFound(expr.toString)
            case Some(slot) =>
              val index = slot.index

              slot.content.cypherType.material match {
                case _ if r.isNullAt(index) => null
                case CTBoolean => cypherBoolean(r.getBoolean(index))
                case CTInteger => cypherInteger(r.getLong(index))
                case CTString => cypherString(r.getString(index))
                case CTFloat => cypherFloat(r.getDouble(index))
                case _ => throw new NotImplementedError(s"Cannot get value from row having type ${slot.content.cypherType}")
              }
          }
      }
    }
  }
}
