package org.opencypher.spark.impl.spark

import org.apache.spark.sql.types._
import org.opencypher.spark.api.types._

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
