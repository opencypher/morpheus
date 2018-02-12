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
package org.opencypher.caps.impl.spark.convert

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.opencypher.caps.impl.exception.IllegalArgumentException
import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.exception.{IllegalArgumentException, NotImplementedException}

object SparkUtils {

  implicit class NullabilityOps(df: DataFrame) {
    def setNonNullable(columnName: String): DataFrame = {
      val newSchema = StructType(df.schema.map {
        case s@StructField(cn, _, true, _) if cn == columnName => s.copy(nullable = false)
        case other => other
      })
      if (newSchema == df.schema) {
        df
      } else {
        df.sparkSession.createDataFrame(df.rdd, newSchema)
      }
    }
  }

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

  def fromSparkType(dt: DataType, nullable: Boolean): Option[CypherType] = {
    val result = dt match {
      case StringType => Some(CTString)
      case LongType => Some(CTInteger)
      case BooleanType => Some(CTBoolean)
      case BinaryType => Some(CTAny)
      case DoubleType => Some(CTFloat)
      case ArrayType(elemType, containsNull) =>
        val maybeElementType = fromSparkType(elemType, containsNull)
        maybeElementType.map(CTList(_))
      case NullType => Some(CTNull)
      case _ => None
    }

    if (nullable) result.map(_.nullable) else result.map(_.material)
  }

  def toSparkType(ct: CypherType): DataType = ct match {
    case CTNull | CTVoid => NullType
    case _ =>
      ct.material match {
        case CTString => StringType
        case CTInteger => LongType
        case CTBoolean => BooleanType
        case CTAny => BinaryType
        case CTFloat => DoubleType
        case _: CTNode => LongType
        case _: CTRelationship => LongType
        case CTList(elemType) => ArrayType(toSparkType(elemType), elemType.isNullable)
        case x =>
          throw NotImplementedException(s"Mapping of CypherType $x to Spark type")
      }
  }

  /**
    * Checks if the given data type is supported within the Cypher type system.
    *
    * @param dataType data type
    * @return true, iff the data type is supported
    */
  def isCypherCompatible(dataType: DataType): Boolean = dataType match {
    case ArrayType(internalType, _) => isCypherCompatible(internalType)
    case other => supportedTypes.contains(other)
  }

  /**
    * Converts the given Spark data type into a Cypher type system compatible Spark data type.
    *
    * @param dataType Spark data type
    * @return some Cypher-compatible Spark data type or none if not compatible
    */
  def cypherCompatibleDataType(dataType: DataType): Option[DataType] = dataType match {
    case ByteType | ShortType | IntegerType => Some(LongType)
    case FloatType => Some(DoubleType)
    case compatible if fromSparkType(dataType, nullable = false).isDefined => Some(compatible)
    case _ => None
  }

  /**
    * Returns the corresponding Cypher type for the given column name in the data frame.
    *
    * @param dataFrame  data frame
    * @param columnName column name
    * @return Cypher type for column
    */
  def cypherTypeForColumn(dataFrame: DataFrame, columnName: String): CypherType = {
    val structField = structFieldForColumn(dataFrame, columnName)
    val compatibleCypherType = cypherCompatibleDataType(structField.dataType).flatMap(fromSparkType(_, structField.nullable))
    compatibleCypherType.getOrElse(
      throw IllegalArgumentException("a supported Spark DataType that can be converted to CypherType", structField.dataType))
  }

  /**
    * Returns the struct field for the given column.
    *
    * @param dataFrame  data frame
    * @param columnName column name
    * @return struct field
    */
  def structFieldForColumn(dataFrame: DataFrame, columnName: String): StructField = {
    if (dataFrame.schema.fieldIndex(columnName) < 0) {
      throw IllegalArgumentException(s"column with name $columnName", s"columns with names ${dataFrame.columns.mkString("[", ", ", "]")}")
    }
    dataFrame.schema.fields(dataFrame.schema.fieldIndex(columnName))
  }

  /**
    * Checks if the data type of the given column is compatible to the given data type.
    *
    * @param dataFrame    data frame
    * @param columnName   column to be checked
    * @param expectedType excepted data type
    */
  def verifyColumnType(dataFrame: DataFrame, columnName: String, expectedType: DataType): Unit = {
    val columnDataType = structFieldForColumn(dataFrame, columnName).dataType
    val compatibleType = cypherCompatibleDataType(columnDataType).getOrElse(throw IllegalArgumentException(
      s"column with name $columnName being type compatible to $expectedType", columnDataType))

    compatibleType match {
      case `expectedType` => ()
      case other => throw IllegalArgumentException(
        s"column with name $columnName being type compatible to $expectedType", other)
    }
  }
}
