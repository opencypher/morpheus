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
    case CTNode => LongType
    case CTRelationship => LongType
    case x => throw new NotImplementedError(s"Cannot map CypherType $x to a suitable Apache Spark DataType")
  }
}

object fromSparkType extends Serializable {
  def apply(dt: DataType, nullable: Boolean) = {
    val result = dt match {
      case _: StringType => CTString
      case _: IntegerType => CTInteger
      case _: BooleanType => CTBoolean
      case _: BinaryType => CTAny
      case _: DoubleType => CTFloat
      case x => throw new NotImplementedError(s"Cannot map Apache Spark DataType $x to a suitable CypherType")
    }
    if (nullable) result.nullable else result.material
  }
}
