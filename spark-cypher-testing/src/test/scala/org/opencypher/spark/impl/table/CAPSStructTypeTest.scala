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
package org.opencypher.spark.impl.table

import org.apache.spark.sql.types._
import org.opencypher.okapi.api.types.{CTInteger, CTList, CTString}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.spark.impl.convert.SparkConversions._

class CAPSStructTypeTest extends BaseTestSuite {

  it("computes a header from a given struct type") {
    val structType = StructType(Seq(
      StructField("a", LongType, nullable = true),
      StructField("b", StringType, nullable = false),
      StructField("c", ArrayType(StringType, containsNull = true), nullable = false)
    ))

    structType.toRecordHeader should equal(RecordHeader(Map(
      Var("a")(CTInteger.nullable) -> "a",
      Var("b")(CTString) -> "b",
      Var("c")(CTList(CTString.nullable)) -> "c"
    )
    ))
  }

}
