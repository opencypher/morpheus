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
package org.opencypher.spark.testing.impl.convert

import org.apache.spark.sql.types._
import org.opencypher.okapi.api.types._
import org.opencypher.spark.impl.convert.CAPSCypherType._
import org.scalatest.{FunSpec, Matchers}

class CAPSCypherTypeTest extends FunSpec with Matchers {

  it("should produce the correct StructField for non-nested types") {
    CTInteger.toStructField("foo") should equal(StructField("foo", LongType, nullable = false))
    CTIntegerOrNull.toStructField("foo") should equal(StructField("foo", LongType, nullable = true))
    CTFloat.toStructField("foo") should equal(StructField("foo", DoubleType, nullable = false))
    CTFloatOrNull.toStructField("foo") should equal(StructField("foo", DoubleType, nullable = true))
    CTBoolean.toStructField("foo") should equal(StructField("foo", BooleanType, nullable = false))
    CTBooleanOrNull.toStructField("foo") should equal(StructField("foo", BooleanType, nullable = true))
    CTString.toStructField("foo") should equal(StructField("foo", StringType, nullable = false))
    CTStringOrNull.toStructField("foo") should equal(StructField("foo", StringType, nullable = true))

    CTNode(Set("A")).toStructField("foo") should equal(StructField("foo", LongType, nullable = false))
    CTNodeOrNull(Set("A")).toStructField("foo") should equal(StructField("foo", LongType, nullable = true))
    CTRelationship("A").toStructField("foo") should equal(StructField("foo", LongType, nullable = false))
    CTRelationshipOrNull("A").toStructField("foo") should equal(StructField("foo", LongType, nullable = true))
  }

  it("should produce the correct StructField for nested types") {
    CTList(CTInteger).toStructField("foo") should equal(StructField("foo", ArrayType(LongType, containsNull = false), nullable = false))
    CTList(CTInteger.nullable).toStructField("foo") should equal(StructField("foo", ArrayType(LongType, containsNull = true), nullable = false))
    CTList(CTInteger).nullable.toStructField("foo") should equal(StructField("foo", ArrayType(LongType, containsNull = false), nullable = true))
    CTList(CTInteger.nullable).nullable.toStructField("foo") should equal(StructField("foo", ArrayType(LongType, containsNull = true), nullable = true))

    CTList(CTList(CTInteger)).toStructField("foo") should equal(
      StructField("foo", ArrayType(ArrayType(LongType, containsNull = false), containsNull = false), nullable = false)
    )

    CTList(CTList(CTInteger.nullable)).toStructField("foo") should equal(
      StructField("foo", ArrayType(ArrayType(LongType, containsNull = true), containsNull = false), nullable = false)
    )

    CTList(CTList(CTInteger.nullable).nullable).toStructField("foo") should equal(
      StructField("foo", ArrayType(ArrayType(LongType, containsNull = true), containsNull = true), nullable = false)
    )

    CTList(CTList(CTInteger.nullable).nullable).nullable.toStructField("foo") should equal(
      StructField("foo", ArrayType(ArrayType(LongType, containsNull = true), containsNull = true), nullable = true)
    )
  }

  it("should throw for unsupported CypherTypes") {
    a[org.opencypher.okapi.impl.exception.IllegalArgumentException] shouldBe thrownBy {
      CTMap.toStructField("foo")
    }
  }

}
