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
package org.opencypher.spark.impl

import java.util.Collections

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.table._
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api.expr.Expr._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.io.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.CAPSEntityTable
import org.opencypher.spark.api.io.SparkCypherTable._
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.convert.rowToCypherMap

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object CAPSRecords {

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit caps: CAPSSession): CAPSRecords = {
    val initialSparkStructType = initialHeader.toStructType
    val initialDataFrame = caps.sparkSession.createDataFrame(Collections.emptyList[Row](), initialSparkStructType)
    CAPSRecords(initialHeader, initialDataFrame)
  }

  def unit()(implicit caps: CAPSSession): CAPSRecords = {
    val initialDataFrame = caps.sparkSession.createDataFrame(Seq(EmptyRow()))
    CAPSRecords(RecordHeader.empty, initialDataFrame)
  }

  def create(entityTable: CAPSEntityTable)(implicit caps: CAPSSession): CAPSRecords = {
    val withCypherCompatibleTypes = entityTable.table.df.withCypherCompatibleTypes
    CAPSRecords(entityTable.header, withCypherCompatibleTypes)
  }

  /**
    * Wraps a Spark SQL table (DataFrame) in a CAPSRecords, making it understandable by Cypher.
    *
    * @param df   table to wrap.
    * @param caps session to which the resulting CAPSRecords is tied.
    * @return a Cypher table.
    */
  private[spark] def wrap(df: DataFrame)(implicit caps: CAPSSession): CAPSRecords = {
    val compatibleDf = df.withCypherCompatibleTypes
    CAPSRecords(compatibleDf.schema.toRecordHeader, compatibleDf)
  }

  private case class EmptyRow()
}

case class CAPSRecords(
  header: RecordHeader,
  df: DataFrame,
  override val logicalColumns: Option[Seq[String]] = None)(implicit val caps: CAPSSession)
  extends RelationalCypherRecords[DataFrameTable]
    with RecordBehaviour
    with Serializable {

  override type R = CAPSRecords

  verify()

  override def from(header: RecordHeader, table: DataFrameTable, displayNames: Option[Seq[String]]): CAPSRecords =
    copy(header, table.df, displayNames)

  override def table: DataFrameTable = df

  //noinspection AccessorLikeMethodIsEmptyParen
  def toDF(colNames: String*): DataFrame = df.toDF(colNames: _*)



  def cache(): CAPSRecords = {
    df.cache()
    this
  }

  def persist(): CAPSRecords = {
    df.persist()
    this
  }

  def persist(storageLevel: StorageLevel): CAPSRecords = {
    df.persist(storageLevel)
    this
  }

  def unpersist(): CAPSRecords = {
    df.unpersist()
    this
  }

  def unpersist(blocking: Boolean): CAPSRecords = {
    df.unpersist(blocking)
    this
  }

  // TODO: Further optimize identity retaggings
  def retag(replacements: Map[Int, Int]): CAPSRecords = {
    val actualRetaggings = replacements.filterNot { case (from, to) => from == to }
    val idColumns = header.idColumns()
    val dfWithReplacedTags = idColumns.foldLeft(df) {
      case (df, column) => df.safeReplaceTags(column, actualRetaggings)
    }
    copy(header, dfWithReplacedTags)
  }

  def retagVariable(v: Var, replacements: Map[Int, Int]): CAPSRecords = {
    val columnsToUpdate = header.idColumns(v)
    val updatedData = columnsToUpdate.foldLeft(df) { case (df, columnName) =>
      df.safeReplaceTags(columnName, replacements)
    }
    copy(header, updatedData)
  }

  protected val TRUE_LIT: Column = functions.lit(true)
  protected val FALSE_LIT: Column = functions.lit(false)
  protected val NULL_LIT: Column = functions.lit(null)

  /**
    * Align the record header to the target header and rename the stored entity to `v`.
    *
    * It is required that the `CAPSRecords` instance is a scan, meaning that it must contain exactly a single entity
    * (node or relationship) and its parts (flattened). The stored entity is renamed by this function to the argument
    * variable `v`.
    *
    * @param v            the variable that the aligned scan should contain
    * @param targetHeader the header to align with
    * @return a new instance of `CAPSRecords` aligned with the argument header
    */
  def alignWith(v: Var, targetHeader: RecordHeader): CAPSRecords = {

    val entityVars = header.entityVars

    val oldEntity = entityVars.toSeq match {
      case Seq(one) => one
      case Nil => throw IllegalArgumentException("one entity in the record header", s"no entity in $header")
      case _ => throw IllegalArgumentException("one entity in the record header", s"multiple entities in $header")
    }

    assert(header.ownedBy(oldEntity) == header.expressions, s"header describes more than one entity")

    val entityLabels: Set[String] = oldEntity.cypherType match {
      case CTNode(labels, _) => labels
      case CTRelationship(types, _) => types
      case _ => throw IllegalArgumentException("CTNode or CTRelationship", oldEntity.cypherType)
    }

    val updatedHeader = header
      .withAlias(oldEntity as v)
      .select(v)

    val missingExpressions = targetHeader.expressions -- updatedHeader.expressions
    val overlapExpressions = targetHeader.expressions -- missingExpressions

    // rename existing columns according to target header
    val dataWithColumnsRenamed = overlapExpressions.foldLeft(df) {
      case (currentDf, expr) =>
        val oldColumn = updatedHeader.column(expr)
        val newColumn = targetHeader.column(expr)
        if (oldColumn != newColumn) {
          currentDf
            .safeReplaceColumn(oldColumn, currentDf.col(oldColumn).cast(expr.cypherType.toSparkType.get))
            .safeRenameColumn(oldColumn, newColumn)
        } else {
          currentDf
        }
    }

    // add missing columns
    val (dataWithMissingColumns, _) = missingExpressions.foldLeft(dataWithColumnsRenamed -> updatedHeader) {
      case ((currentDf, currentHeader), expr) =>
        val newColumn = expr match {
          case HasLabel(_, label) => if (entityLabels.contains(label.name)) TRUE_LIT else FALSE_LIT
          case HasType(_, relType) => if (entityLabels.contains(relType.name)) TRUE_LIT else FALSE_LIT
          case _ =>
            if (!expr.cypherType.isNullable) {
              throw UnsupportedOperationException(
                s"Cannot align scan on $v by adding a NULL column, because the type for '$expr' is non-nullable"
              )
            }
            NULL_LIT.cast(expr.cypherType.getSparkType)
        }
        val headerWithColumn = currentHeader.withExpr(expr)

        currentDf.withColumn(
          headerWithColumn.column(expr), // TODO: possible mismatch between column name in updated header and new column name
          newColumn.as(targetHeader.column(expr))) -> headerWithColumn
    }
    copy(targetHeader, dataWithMissingColumns)
  }

  private def verify(): Unit = {

    @tailrec
    def containsEntity(t: CypherType): Boolean = t match {
      case _: CTNode => true
      case _: CTRelationship => true
      case l: CTList => containsEntity(l.elementType)
      case _ => false
    }

    if (df.sparkSession != caps.sparkSession) {
      throw IllegalArgumentException(
        "a DataFrame belonging to the same Spark session",
        "DataFrame from different session")
    }

    // Ensure no duplicate columns in initialData
    val initialDataColumns = df.columns.toSeq

    val duplicateColumns = initialDataColumns.groupBy(identity).collect {
      case (key, values) if values.size > 1 => key
    }

    if (duplicateColumns.nonEmpty)
      throw IllegalArgumentException(
        "a DataFrame with distinct columns",
        s"a DataFrame with duplicate columns: ${initialDataColumns.sorted.mkString("[", ", ", "]")}")

    // Verify that all header column names exist in the data
    val headerColumnNames = header.columns
    val dataColumnNames = df.columns.toSet
    val missingColumnNames = headerColumnNames -- dataColumnNames
    if (missingColumnNames.nonEmpty) {
      throw IllegalArgumentException(
        s"data with columns ${header.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
        s"data with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}"
      )
    }

    // Verify column types
    header.expressions.foreach { expr =>
      val structType = df.schema
      val structField = structType(header.column(expr))
      val cypherType = structField.toCypherType
        .getOrElse(throw IllegalArgumentException("a supported Spark type", structField.dataType))
      val headerType = expr.cypherType
      // if the type in the data doesn't correspond to the type in the header we fail
      // except: we encode nodes, rels and integers with the same data type, so we can't fail
      // on conflicts when we expect entities (alternative: change reverse-mapping function somehow)
      if (headerType.toSparkType != cypherType.toSparkType && !containsEntity(headerType))
        throw IllegalArgumentException(s"a valid data type for column ${structField.name} of type $headerType", cypherType)
    }
  }

  override def toString: String = {
    val numRows = df.size
    if (header.isEmpty && numRows == 0) {
      s"CAPSRecords.empty"
    } else if (header.isEmpty && numRows == 1) {
      s"CAPSRecords.unit"
    } else {
      s"CAPSRecords($header, table with $numRows rows)"
    }
  }
}

trait RecordBehaviour extends RelationalCypherRecords[DataFrameTable] {

  override def show(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  override lazy val columnType: Map[String, CypherType] = table.df.columnType

  override def rows: Iterator[String => CypherValue] = {
    toLocalIterator.asScala.map(_.value)
  }

  override def iterator: Iterator[CypherMap] = {
    toLocalIterator.asScala
  }

  def toLocalIterator: java.util.Iterator[CypherMap] = {
    toCypherMaps.toLocalIterator()
  }

  def foreachPartition(f: Iterator[CypherMap] => Unit): Unit = {
    toCypherMaps.foreachPartition(f)
  }

  override def collect: Array[CypherMap] =
    toCypherMaps.collect()

  override def size: Long = table.df.count()

  def toCypherMaps: Dataset[CypherMap] = {
    import encoders._
    table.df.map(rowToCypherMap(header.exprToColumn.toSeq))
  }
}
