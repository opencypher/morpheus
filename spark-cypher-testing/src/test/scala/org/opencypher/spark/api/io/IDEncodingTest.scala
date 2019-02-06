/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.api.io

import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.functions.typedLit
import org.opencypher.spark.api.io.IDEncoding._
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalacheck.Prop.propBoolean
import org.scalatest.prop.Checkers

class IDEncodingTest extends CAPSTestSuite with Checkers {

  it("encodes longs correctly") {
    check((l: Long) => {
      val scala = l.encodeAsCAPSId.toList
      val spark = typedLit[Long](l).encodeLongAsCAPSId.expr.eval().asInstanceOf[Array[Byte]].toList
      scala === spark
    }, minSuccessful(1000))
  }

  it("encoding/decoding is symmetric") {
    check((l: Long) => {
      val encoded = l.encodeAsCAPSId
      val decoded = encoded.decodeToLong
      decoded === l
    }, minSuccessful(1000))
  }

  it("scala version encodes longs correctly") {
    0L.encodeAsCAPSId.toList should equal(List.fill(8)(0.toByte))
    (-1L).encodeAsCAPSId.toList should equal(List.fill(8)(-1.toByte))
  }

  it("spark version encodes longs correctly") {
    typedLit[Long](0L).encodeLongAsCAPSId.expr.eval().asInstanceOf[GenericArrayData].array.toList should equal(List.fill(8)(0.toByte))
  }

}
