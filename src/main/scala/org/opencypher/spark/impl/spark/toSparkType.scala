package org.opencypher.spark.impl.spark

import org.apache.spark.sql.types._
import org.opencypher.spark.api.types._

object toSparkType extends Serializable {
  def apply(ct: CypherType): DataType = ct.material match {
    case CTString => StringType
    case CTInteger => LongType
    case CTBoolean => BooleanType
    case CTAny => BinaryType
    case CTFloat => DoubleType
    case _: CTNode => LongType
    case _: CTRelationship => LongType
    case x => throw new NotImplementedError(s"No mapping for $x")
  }
}

