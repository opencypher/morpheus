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
package org.opencypher.okapi.impl.spark

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.impl.table.{ColumnName, RecordHeader}
import org.opencypher.okapi.impl.spark.physical.CAPSRuntimeContext
import org.opencypher.okapi.ir.api.expr.{Expr, Param}

object DataFrameOps {

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

  implicit class CypherRow(r: Row) {
    def getCypherValue(expr: Expr, header: RecordHeader)(implicit context: CAPSRuntimeContext): CypherValue = {
      expr match {
        case Param(name) => context.parameters(name)
        case _ =>
          header.slotsFor(expr).headOption match {
            case None => throw IllegalArgumentException(s"slot for $expr")
            case Some(slot) =>
              val index = slot.index
              CypherValue(r.get(index))
          }
      }
    }
  }

  implicit class RichDataFrame(val df: DataFrame) extends AnyVal {

    /**
      * Returns the corresponding Cypher type for the given column name in the data frame.
      *
      * @param columnName column name
      * @return Cypher type for column
      */
    def cypherTypeForColumn(columnName: String): CypherType = {
      val structField = structFieldForColumn(columnName)
      val compatibleCypherType = cypherCompatibleDataType(structField.dataType).flatMap(fromSparkType(_, structField.nullable))
      compatibleCypherType.getOrElse(
        throw IllegalArgumentException("a supported Spark DataType that can be converted to CypherType", structField.dataType))
    }

    /**
      * Returns the struct field for the given column.
      *
      * @param columnName column name
      * @return struct field
      */
    def structFieldForColumn(columnName: String): StructField = {
      if (df.schema.fieldIndex(columnName) < 0) {
        throw IllegalArgumentException(s"column with name $columnName", s"columns with names ${df.columns.mkString("[", ", ", "]")}")
      }
      df.schema.fields(df.schema.fieldIndex(columnName))
    }

    def mapColumn(name: String)(f: Column => Column): DataFrame = {
      df.withColumn(name, f(df.col(name)))
    }

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

    def safeAddColumn(name: String, col: Column): DataFrame = {
      require(!df.columns.contains(name),
        s"Cannot add column `$name`. A column with that name exists already. " +
          s"Use `safeReplaceColumn` if you intend to replace that column.")
      df.withColumn(name, col)
    }

    def safeReplaceColumn(name: String, newColumn: Column): DataFrame = {
      require(df.columns.contains(name), s"Cannot replace column `$name`. No column with that name exists. " +
        s"Use `safeAddColumn` if you intend to add that column.")
      df.withColumn(name, newColumn)
    }

    def safeRenameColumn(oldName: String, newName: String): DataFrame = {
      require(!df.columns.contains(newName),
        s"Cannot rename column `$oldName` to `$newName`. A column with name `$newName` exists already.")
      df.withColumnRenamed(oldName, newName)
    }

    def safeDropColumn(name: String): DataFrame = {
      require(df.columns.contains(name),
        s"Cannot drop column `$name`. No column with that name exists.")
      df.drop(name)
    }

    def safeDropColumns(names: String*): DataFrame = {
      val nonExistentColumns = names.toSet -- df.columns
      require(nonExistentColumns.isEmpty,
        s"Cannot drop column(s) ${nonExistentColumns.map(c => s"`$c`").mkString(", ")}. They do not exist.")
      df.drop(names: _*)
    }

    def safeJoin(other: DataFrame, joinCols: Seq[(String, String)], joinType: String): DataFrame = {
      require(joinCols.map(_._1).forall(col => !other.columns.contains(col)))
      require(joinCols.map(_._2).forall(col => !df.columns.contains(col)))

      val joinExpr = joinCols.map {
        case (l, r) => df.col(l) === other.col(r)
      }.foldLeft(functions.lit(true))((acc, expr) => acc && expr)

      df.join(other, joinExpr, joinType)
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

}
