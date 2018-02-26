/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.okapi.impl.convert

import org.apache.spark.sql.types._
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.impl.spark.DataFrameOps._
import org.opencypher.okapi.test.BaseTestSuite

class ConvertersTest extends BaseTestSuite {

  test("converts from spark types to cypher types") {
    val mappings = Seq(
      LongType -> CTInteger,
      DoubleType -> CTFloat,
      StringType -> CTString,
      BooleanType -> CTBoolean,
      ArrayType(LongType, containsNull = false) -> CTList(CTInteger),
      ArrayType(StringType, containsNull = true) -> CTList(CTString.nullable),
      BinaryType -> CTAny
    )

    mappings.foreach {
      case (spark, cypher) =>
        fromSparkType(spark, nullable = false) should equal(Some(cypher))
        fromSparkType(spark, nullable = true) should equal(Some(cypher.nullable))
    }
  }

  test("does not support detailed number types") {
    val unsupported = Set(FloatType, ShortType, ByteType)

    unsupported.foreach { t =>
      fromSparkType(t, nullable = false) should equal(None)
    }
  }

  test("converts from cypher types to spark types") {
    val mappings = Seq(
      CTInteger -> LongType,
      CTFloat -> DoubleType,
      CTString -> StringType,
      CTBoolean -> BooleanType,
      CTList(CTInteger) -> ArrayType(LongType, containsNull = false),
      CTList(CTString.nullable) -> ArrayType(StringType, containsNull = true),
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

  test("converts from spark values to cypher types") {
    val mappings = Seq(
      "string" -> CTString,
      Integer.valueOf(1) -> CTInteger,
      java.lang.Long.valueOf(1) -> CTInteger,
      java.lang.Short.valueOf("1") -> CTInteger,
      java.lang.Byte.valueOf("1") -> CTInteger,
      java.lang.Double.valueOf(3.14) -> CTFloat,
      java.lang.Float.valueOf(3.14f) -> CTFloat,
      java.lang.Boolean.TRUE -> CTBoolean,
      Array(1) -> CTList(CTInteger),
      Array() -> CTList(CTVoid),
      Array(Int.box(1), Double.box(3.14)) -> CTList(CTNumber),
      Array(null, "foo") -> CTList(CTString.nullable)
    )

    mappings.foreach {
      case (spark, cypher) =>
        CypherValue(spark).cypherType should equal(cypher)
    }
  }
}
