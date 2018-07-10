/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl.convert

import org.apache.spark.sql.types._
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.spark.impl.convert.SparkConversions._

class ConvertersTest extends BaseTestSuite {

  it("can convert from spark types to cypher types") {
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
        spark.toCypherType(false) should equal(Some(cypher))
        spark.toCypherType(true) should equal(Some(cypher.nullable))
    }
  }

  it("does not support detailed number types") {
    val unsupported = Set(FloatType, ShortType, ByteType)

    unsupported.foreach { t =>
      t.toCypherType() should equal(None)
    }
  }

  it("can convert cypher types to spark types") {
    val mappings = Seq(
      CTInteger -> LongType,
      CTFloat -> DoubleType,
      CTString -> StringType,
      CTBoolean -> BooleanType,
      CTList(CTInteger) -> ArrayType(LongType, containsNull = false),
      CTList(CTString.nullable) -> ArrayType(StringType, containsNull = true),
      CTAnyNode -> LongType,
      CTNode("Foo") -> LongType,
      CTAnyRelationship -> LongType,
      CTRelationship("BAR") -> LongType
    )

    mappings.foreach {
      case (cypher, spark) =>
        cypher.getSparkType should equal(spark)
    }
  }

  it("can convert from spark values to cypher types") {
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
