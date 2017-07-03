package org.opencypher.spark.impl.convert

import org.apache.spark.sql.types._
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.exception.Raise

object toSparkType extends Serializable {

  def apply(ct: CypherType): DataType = ct.material match {
    case CTString => StringType
    case CTInteger => LongType
    case CTBoolean => BooleanType
    case CTAny => BinaryType
    case CTFloat => DoubleType
    case _: CTNode => LongType
    case _: CTRelationship => LongType
    case CTList(elemType) => ArrayType(toSparkType(elemType), elemType.isNullable)
    case x =>
      Raise.notYetImplemented(s"mapping of CypherType $x to Spark type")
  }
}

