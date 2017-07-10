package org.opencypher.spark.impl.convert

import org.apache.spark.sql.types._
import org.opencypher.spark.BaseTestSuite
import org.opencypher.spark.api.types._

class ConvertersTest extends BaseTestSuite {

  test("converts from spark types to cypher types") {
    val mappings = Seq(
      LongType -> CTInteger,
      IntegerType -> CTInteger,
      ShortType -> CTInteger,
      ByteType -> CTInteger,
      DoubleType -> CTFloat,
      FloatType -> CTFloat,
      StringType -> CTString,
      BooleanType -> CTBoolean,
      ArrayType(LongType, containsNull = false) -> CTList(CTInteger),
      ArrayType(StringType, containsNull = true) -> CTList(CTStringOrNull),
      BinaryType -> CTAny
    )

    mappings.foreach {
      case (spark, cypher) =>
        fromSparkType(spark, nullable = false) should equal(cypher)
        fromSparkType(spark, nullable = true) should equal(cypher.nullable)
    }
  }

  test("converts from cypher types to spark types") {
    val mappings = Seq(
      CTInteger -> LongType,
      CTFloat -> DoubleType,
      CTString -> StringType,
      CTBoolean -> BooleanType,
      CTList(CTInteger) -> ArrayType(LongType, containsNull = false),
      CTList(CTStringOrNull) -> ArrayType(StringType, containsNull = true),
      CTAny -> BinaryType,
      CTNode -> LongType,
      CTNode("Foo") -> LongType,
      CTRelationship -> LongType,
      CTRelationship("BAR") -> LongType
    )

    mappings.foreach {
      case (cypher, spark) =>
        toSparkType(cypher) should equal(spark)
    }
  }

  // TODO: container types
  test("converts from spark values to cypher types") {
    val mappings = Seq(
      "string" -> CTString,
      Integer.valueOf(1) -> CTInteger,
      java.lang.Long.valueOf(1) -> CTInteger,
      java.lang.Short.valueOf("1") -> CTInteger,
      java.lang.Byte.valueOf("1") -> CTInteger,
      java.lang.Double.valueOf(3.14) -> CTFloat,
      java.lang.Float.valueOf(3.14f) -> CTFloat,
      java.lang.Boolean.TRUE -> CTBoolean
    )

    mappings.foreach {
      case (spark, cypher) =>
        fromJavaType(spark) should equal(cypher)
    }
  }
}
