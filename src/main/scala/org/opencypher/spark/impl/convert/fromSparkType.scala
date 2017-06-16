package org.opencypher.spark.impl.convert

import org.apache.spark.sql.types._
import org.opencypher.spark.api.types._

object fromSparkType extends Serializable {

  def apply(dt: DataType, nullable: Boolean): CypherType = {
    val result = dt match {
      case _: StringType => CTString
      case IntegerType | LongType | ShortType | ByteType => CTInteger
      case _: BooleanType => CTBoolean
      case _: BinaryType => CTAny
      case DoubleType | FloatType => CTFloat
      case ArrayType(elemType, containsNull) => CTList(fromSparkType(elemType, containsNull))
      case x => throw new NotImplementedError(s"Cannot map Apache Spark DataType $x to a suitable CypherType")
    }

    if (nullable) result.nullable else result.material
  }
}
