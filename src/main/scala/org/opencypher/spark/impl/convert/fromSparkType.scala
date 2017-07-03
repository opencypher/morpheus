package org.opencypher.spark.impl.convert

import org.apache.spark.sql.types._
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.exception.Raise

object fromSparkType extends Serializable {

  def apply(dt: DataType, nullable: Boolean): CypherType = {
    val result = dt match {
      case _: StringType => CTString
      case IntegerType | LongType | ShortType | ByteType => CTInteger
      case _: BooleanType => CTBoolean
      case _: BinaryType => CTAny
      case DoubleType | FloatType => CTFloat
      case ArrayType(elemType, containsNull) => CTList(fromSparkType(elemType, containsNull))
      case x =>
        Raise.notYetImplemented(s"mapping of Apache Spark DataType $x")
    }

    if (nullable) result.nullable else result.material
  }
}
