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

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{StructField, StructType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.relational.impl.table.RecordHeader._
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.impl.convert.CAPSCypherType._

object CAPSRecordHeader {

  def fromSparkStructType(structType: StructType): RecordHeader =
    RecordHeader.apply(structType.fields.map { field =>
        Var(field.name)(field.dataType.toCypherType(field.nullable)
          .getOrElse(throw IllegalArgumentException("a supported Spark type", field.dataType)))
    }: _*)

  implicit class CAPSRecordHeader(header: RecordHeader) extends Serializable {

    def columnMappings: Map[String, StructField] = {
      header.mappings.map { slot =>
        slot.columnName -> slot.asStructField
      }.toMap
    }

    def sparkSchemaForDf(df: DataFrame): StructType = {
      val schemaForColumn = columnMappings
      StructType(df.columns.map(schemaForColumn))
    }

    def rowEncoder(implicit df: DataFrame): ExpressionEncoder[Row] = RowEncoder(sparkSchemaForDf(df))

    def columns: Set[String] = header.mappings.map(_.columnName)

    def dfColumnNameToSlotContent: Map[String, ExpressionMapping] = {
      for {
        slot <- header.mappings
      } yield slot.columnName -> slot
    }.toMap

    def dfColumnNameToFieldName: Map[String, String] = {
      for {
        field <- header.fields
        slot = header.exprFor(field)
      } yield slot.columnName -> field.name
    }.toMap
  }

  implicit class CAPSOrderedSlots(slotContents: Seq[Expr]) {

    def asSparkSchema: StructType = StructType(slotContents.map(_.asStructField))

  }

  implicit class CAPSRecordSlot(slot: ExpressionMapping) {

    def asStructField: StructField = slot.expr.asStructField

    def columnName: String = ColumnName.of(slot)

    def indexIn(df: DataFrame): Int = {
      df.schema.fieldIndex(slot.columnName)
    }

    def columnIn(df: DataFrame): Column = {
      df.col(columnName)
    }

  }

  implicit class CAPSSlotContent(slotContent: Expr) {

    def asStructField: StructField = {
      val name = slotContent.columnName
      val sparkType = slotContent.cypherType.getSparkType
      StructField(name, sparkType, slotContent.cypherType.isNullable)
    }

    def columnName: String = ColumnName.of(slotContent)

  }

}
