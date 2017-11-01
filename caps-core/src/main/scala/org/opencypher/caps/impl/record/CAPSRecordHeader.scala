/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.record

import org.apache.spark.sql.types.{StructField, StructType}
import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.{OpaqueField, RecordHeader, RecordSlot}
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.convert.{fromSparkType, toSparkType}
import org.opencypher.caps.impl.spark.exception.Raise

object CAPSRecordHeader {

  def fromSparkStructType(structType: StructType): RecordHeader =
    RecordHeader.from(structType.fields.map { field =>
      OpaqueField(
        Var(field.name)(fromSparkType(field.dataType, field.nullable)
          .getOrElse(Raise.invalidArgument("A supported Spark type", field.dataType.toString))))
    }: _*)

  def asSparkStructType(header: RecordHeader): StructType = {
    val fields = header.slots.map(slot => structField(slot, !header.mandatory(slot)))
    StructType(fields)
  }

  private def structField(slot: RecordSlot, nullable: Boolean): StructField = {
    val name     = SparkColumnName.of(slot.content)
    val dataType = toSparkType(slot.content.cypherType)
    StructField(name, dataType, nullable)
  }
}
