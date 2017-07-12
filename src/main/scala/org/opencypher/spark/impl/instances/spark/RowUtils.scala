package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.Row
import org.opencypher.spark.api.expr.{Const, Expr}
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.value._
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

              if (r.isNullAt(index))
                null
              else typeToValue(slot.content.cypherType.material)(r.get(index))
          }
      }
    }

    def typeToValue(t: CypherType): Any => CypherValue = t match {
      case CTBoolean => (in) => cypherBoolean(in.asInstanceOf[Boolean])
      case CTInteger => (in) => cypherInteger(in.asInstanceOf[Long])
      case CTString => (in) => cypherString(in.asInstanceOf[String])
      case CTFloat => (in) => cypherFloat(in.asInstanceOf[Double])
        // TODO: This supports var-expand where we only track rel ids, but it's not right
      case CTRelationship => (in) => cypherInteger(in.asInstanceOf[Long])
      case l: CTList => (in) => {
        val converted = in.asInstanceOf[Seq[_]].map(typeToValue(l.elementType))

        cypherList(converted.toIndexedSeq)
      }
      case _ => Raise.notYetImplemented(s"converting value of type $t")
    }
  }
}
