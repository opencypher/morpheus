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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.impl.convert.CAPSCypherType._

object CAPSRecordHeader {

  def fromSparkStructType(structType: StructType): RecordHeader =
    RecordHeader.from(structType.fields.map { field =>
      OpaqueField(
        Var(field.name)(field.dataType.toCypherType(field.nullable)
          .getOrElse(throw IllegalArgumentException("a supported Spark type", field.dataType))))
    }: _*)

  def asSparkStructType(header: RecordHeader): StructType = {
    StructType(header.slots.map(slot => slot.content.cypherType.toStructField(header.of(slot.content))))
  }

  // TODO: Move to RecordHeader itself
  implicit class CAPSRecordHeader(header: RecordHeader) extends Serializable {

    def rowEncoder: ExpressionEncoder[Row] =
      RowEncoder(asSparkStructType(header))

    def columns = header.internalHeader.slots.map(computeColumnName).toVector

    def column(slot: RecordSlot) = columns(slot.index)

    private def computeColumnName(slot: RecordSlot): String = header.of(slot)
  }
}
