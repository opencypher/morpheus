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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{array, shiftRightUnsigned}
import org.apache.spark.sql.types.ByteType
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix

object IDEncoding {

  type CAPSId = Seq[Byte]

  implicit class ColumnIdEncoding(val c: Column) extends AnyVal {

    def encodeLongAsCAPSId(name: String): Column = encodeLongAsCAPSId.as(name)

    def encodeLongAsCAPSId: Column = {
      array(
        shiftRightUnsigned(c, 56).cast(ByteType),
        shiftRightUnsigned(c, 48).cast(ByteType),
        shiftRightUnsigned(c, 40).cast(ByteType),
        shiftRightUnsigned(c, 32).cast(ByteType),
        shiftRightUnsigned(c, 24).cast(ByteType),
        shiftRightUnsigned(c, 16).cast(ByteType),
        shiftRightUnsigned(c, 8).cast(ByteType),
        c.cast(ByteType)
      )
    }

  }

  implicit class LongIdEncoding(val l: Long) extends AnyVal {

    def encodeAsCAPSId: CAPSId = {
      val a = new Array[Byte](8)
      a(0) = (l >> 56).toByte
      a(1) = (l >> 48).toByte
      a(2) = (l >> 40).toByte
      a(3) = (l >> 32).toByte
      a(4) = (l >> 24).toByte
      a(5) = (l >> 16).toByte
      a(6) = (l >> 8).toByte
      a(7) = l.toByte
      a
    }

  }

  implicit class RichCAPSId(val id: CAPSId) extends AnyVal {
    def withPrefix(prefix: GraphIdPrefix): CAPSId = prefix +: id
  }

}
