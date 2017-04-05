package org.opencypher.spark.prototype.impl.spark

import org.apache.spark.sql.types._
import org.opencypher.spark.prototype.api.types._

object sparkType extends Serializable {
  def apply(ct: CypherType): DataType = ct.material match {
    case CTString => StringType
    case CTInteger => LongType
    case CTBoolean => BooleanType
    case CTAny => BinaryType
    case CTFloat => DoubleType
    case x => throw new NotImplementedError(s"No mapping for $x")
  }
}

