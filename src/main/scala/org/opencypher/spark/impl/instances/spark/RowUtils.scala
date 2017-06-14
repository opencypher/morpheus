package org.opencypher.spark.impl.instances.spark

import org.apache.spark.sql.Row
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.api.value.CypherValue.Conversion._

object RowUtils {
  implicit class CypherRow(r: Row) {
    def getCypherValue[T <: CypherType](index: Int, cypherType: T): CypherValue = cypherType.material match {
      case CTBoolean => cypherBoolean(r.getBoolean(index))
      case CTInteger => cypherInteger(r.getLong(index))
      case CTString => cypherString(r.getString(index))
      case CTFloat => cypherFloat(r.getDouble(index))
      case _ => throw new NotImplementedError(s"Cannot get value from row having type $cypherType")
    }
  }
}
