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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader

object SparkConversions {

  // Spark data types that are supported within the Cypher type system
  val supportedTypes = Seq(
    // numeric
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    // other
    StringType,
    BooleanType,
    NullType
  )

  implicit class CypherTypeOps(val ct: CypherType) extends AnyVal {
    def toSparkType: Option[DataType] = ct match {
      case CTNull | CTVoid => Some(NullType)
      case _ =>
        ct.material match {
          case CTString => Some(StringType)
          case CTInteger => Some(LongType)
          case CTBoolean => Some(BooleanType)
          case CTFloat => Some(DoubleType)
          case _: CTNode => Some(LongType)
          case _: CTRelationship => Some(LongType)
          case CTList(CTVoid) => Some(ArrayType(NullType, containsNull = true))
          case CTList(CTNull) => Some(ArrayType(NullType, containsNull = true))
          case CTList(elemType) =>
            elemType.toSparkType.map(ArrayType(_, elemType.isNullable))
          case _ =>
            None
        }
    }

    def getSparkType: DataType = toSparkType match {
      case Some(t) => t
      case None => throw NotImplementedException(s"Mapping of CypherType $ct to Spark type")
    }

    def isSparkCompatible: Boolean = toSparkType.isDefined

    def toStructField(column: String): StructField = ct match {
      case CTVoid | CTNull => StructField(column, NullType, nullable = true)

      case _: CTNode => StructField(column, LongType, nullable = false)
      case _: CTNodeOrNull => StructField(column, LongType, nullable = true)
      case _: CTRelationship => StructField(column, LongType, nullable = false)
      case _: CTRelationshipOrNull => StructField(column, LongType, nullable = true)

      case CTInteger => StructField(column, LongType, nullable = false)
      case CTIntegerOrNull => StructField(column, LongType, nullable = true)
      case CTBoolean => StructField(column, BooleanType, nullable = false)
      case CTBooleanOrNull => StructField(column, BooleanType, nullable = true)
      case CTFloat => StructField(column, DoubleType, nullable = false)
      case CTFloatOrNull => StructField(column, DoubleType, nullable = true)
      case CTString => StructField(column, StringType, nullable = false)
      case CTStringOrNull => StructField(column, StringType, nullable = true)

      case CTList(elementType) =>
        val elementStructField = elementType.toStructField(column)
        StructField(column, ArrayType(elementStructField.dataType, containsNull = elementStructField.nullable), nullable = false)
      case CTListOrNull(elementType) =>
        val elementStructField = elementType.toStructField(column)
        StructField(column, ArrayType(elementStructField.dataType, containsNull = elementStructField.nullable), nullable = true)

      case other => throw IllegalArgumentException("CypherType supported by CAPS", other)
    }
  }

  implicit class StructTypeOps(val structType: StructType) {
    def toRecordHeader: RecordHeader = {

      val exprToColumn = structType.fields.map { field =>
        val cypherType = field.toCypherType match {
          case Some(ct) => ct
          case None => throw IllegalArgumentException("a supported Spark type", field.dataType)
        }
        Var(field.name)(cypherType) -> field.name
      }

      RecordHeader(exprToColumn.toMap)
    }
  }

  implicit class StructFieldOps(val field: StructField) extends AnyVal {
    def toCypherType: Option[CypherType] = field.dataType.toCypherType(field.nullable)
  }

  implicit class DataTypeOps(val dt: DataType) extends AnyVal {
    def toCypherType(nullable: Boolean = false): Option[CypherType] = {
      val result = dt match {
        case StringType => Some(CTString)
        case LongType => Some(CTInteger)
        case BooleanType => Some(CTBoolean)
        case BinaryType => Some(CTAny)
        case DoubleType => Some(CTFloat)
        case ArrayType(NullType, _) => Some(CTList(CTVoid))
        case ArrayType(elemType, containsNull) =>
          elemType.toCypherType(containsNull).map(CTList)
        case NullType => Some(CTNull)
        case _ => None
      }

      if (nullable) result.map(_.nullable) else result.map(_.material)
    }

    /**
      * Checks if the given data type is supported within the Cypher type system.
      *
      * @return true, iff the data type is supported
      */
    def isCypherCompatible: Boolean = dt match {
      case ArrayType(internalType, _) => internalType.isCypherCompatible
      case other => supportedTypes.contains(other)
    }

    /**
      * Converts the given Spark data type into a Cypher type system compatible Spark data type.
      *
      * @return some Cypher-compatible Spark data type or none if not compatible
      */
    def cypherCompatibleDataType: Option[DataType] = dt match {
      case ByteType | ShortType | IntegerType => Some(LongType)
      case FloatType => Some(DoubleType)
      case compatible if dt.toCypherType().isDefined => Some(compatible)
      case _ => None
    }
  }

  implicit class RecordHeaderOps(header: RecordHeader) extends Serializable {

    def toStructType: StructType = {
      val structFields = header.columns.toSeq.sorted.map { column =>
        val expressions = header.expressionsFor(column)
        val commonType = expressions.map(_.cypherType).reduce(_ union _)
        assert(commonType.isSparkCompatible,s"""
         |Expressions $expressions with common super type $commonType mapped to column $column have no compatible data type.
         """.stripMargin)
        commonType.toStructField(column)
      }
      StructType(structFields)
    }

    def rowEncoder: ExpressionEncoder[Row] =
      RowEncoder(header.toStructType)
  }

  implicit class RowOps(row: Row) {

    def allNull: Boolean = allNull(row.size)

    def allNull(rowSize: Int): Boolean = (for (i <- 0 until rowSize) yield row.isNullAt(i)).reduce(_ && _)
  }

}
