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
package org.opencypher.spark.impl

import java.util.Collections

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.table.{CypherRecords, CypherRecordsCompanion}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException}
import org.opencypher.okapi.impl.table._
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.impl.exception.DuplicateSourceColumnException
import org.opencypher.okapi.relational.impl.table.RecordHeader._
import org.opencypher.okapi.relational.impl.table.{ColumnName, _}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.EntityTable._
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.CAPSRecords.{prepareDataFrame, verifyAndCreate}
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.convert.CAPSCypherType._
import org.opencypher.spark.impl.convert.rowToCypherMap
import org.opencypher.spark.impl.table.CAPSRecordHeader._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.reflect.runtime.universe.TypeTag

sealed abstract class CAPSRecords(val header: RecordHeader, val data: DataFrame)
  (implicit val caps: CAPSSession) extends CypherRecords with Serializable {

  override def show(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  override def size: Long = data.count()

  override lazy val columnType: Map[String, CypherType] = data.columnType

  def mapDF(f: DataFrame => DataFrame): CAPSRecords = verifyAndCreate(prepareDataFrame(f(data)))

  def cache(): CAPSRecords = {
    data.cache()
    this
  }

  def persist(): CAPSRecords = {
    data.persist()
    this
  }

  def persist(storageLevel: StorageLevel): CAPSRecords = {
    data.persist(storageLevel)
    this
  }

  def unpersist(): CAPSRecords = {
    data.unpersist()
    this
  }

  def unpersist(blocking: Boolean): CAPSRecords = {
    data.unpersist(blocking)
    this
  }

  // TODO: Forther optimize identity retaggings
  def retag(replacements: Map[Int, Int]): CAPSRecords = {
    val actualRetaggings = replacements.filterNot { case (from, to) => from == to }
    val idColumns: Set[String] = header.expressions.collect {
      case f: Var => f
      case s: StartNode => s
      case e: EndNode => e
    }.map(_.columnName)
    val dfWithReplacedTags = idColumns.foldLeft(data) {
      case (df, column) => df.safeReplaceTags(column, actualRetaggings)
    }

    CAPSRecords.verifyAndCreate(header, dfWithReplacedTags)
  }

  def select(fields: Seq[Var]): CAPSRecords = {
    val selectedHeader = header.selectFields(fields.toSet)
    // To ensure DF ordering
    val orderedColumnMappings: Seq[String] = fields.flatMap(header.selectField(_).expressions.map(_.columnName))
    val selectedColumns = data.select(orderedColumnMappings.head, orderedColumnMappings.tail: _*)
    CAPSRecords.verifyAndCreate(selectedHeader, selectedColumns)
  }

  def addAliases(aliasToOriginal: Map[Var, Var]): CAPSRecords = {
    val updatedHeader = aliasToOriginal.foldLeft(header) {
      case (tempHeader, (nextAlias, nextOriginal)) =>
        tempHeader.withMapping(nextOriginal -> Set(nextAlias))
    }
    CAPSRecords.verifyAndCreate(updatedHeader, data)
  }

  def retagVariable(v: Var, replacements: Map[Int, Int]): CAPSRecords = {
    // TODO: Handle unsafe get
    val slotsToRetag = v.cypherType match {
      case _: CTNode => Set(header.exprFor(v))
      case _: CTRelationship =>
        val idSlot = header.exprFor(v)
        val sourceSlot = header.sourceNodeMapping(v)
        val targetSlot = header.targetNodeMapping(v)
        Set(idSlot, sourceSlot, targetSlot)
      case _ => Set.empty
    }
    val columnsToRetag = slotsToRetag.map(_.columnName)
    val retaggedData = columnsToRetag.foldLeft(data) { case (df, columnName) =>
      df.safeReplaceTags(columnName, replacements)
    }
    CAPSRecords.verifyAndCreate(header, retaggedData)
  }

  def removeVars(vars: Set[Var]): CAPSRecords = {
    val (updatedHeader, updatedData) = vars.foldLeft((header, data)) {
      case ((tempHeader, tempDf), nextFieldToRemove) =>
        val slotsToRemove = tempHeader.selectField(nextFieldToRemove)
        val updatedHeader = tempHeader -- slotsToRemove.keySet
        updatedHeader -> tempDf.drop(slotsToRemove.toSeq.map(ColumnName.of): _*)
    }
    CAPSRecords.verifyAndCreate(updatedHeader, updatedData)
  }

  def unionAll(header: RecordHeader, other: CAPSRecords): CAPSRecords = {
    val unionData = data.union(other.data)
    CAPSRecords.verifyAndCreate(header, unionData)
  }

  def distinct: CAPSRecords = {
    CAPSRecords.verifyAndCreate(header, data.distinct())
  }

  def distinct(fields: Var*): CAPSRecords =
    CAPSRecords.verifyAndCreate(header, data.dropDuplicates(fields.map(_.columnName)))

  /**
    * Converts all values stored in this table to instances of the corresponding CypherValue class.
    * In particular, this de-flattens, or collects, flattened entities (nodes and relationships) into
    * compact CypherNode/CypherRelationship objects.
    *
    * All values on each row are inserted into a CypherMap object mapped to the corresponding field name.
    *
    * @return a dataset of CypherMaps.
    */
  def toCypherMaps: Dataset[CypherMap] = {
    import encoders._
    data.map(rowToCypherMap(header))
  }

  /**
    * Returns the header fields in the order specified by the DataFrame
    *
    * @return columns in header ordered as in the DF
    */
  override lazy val columns: Seq[String] = {
    val dfColumnNameToFieldNameMap = header.dfColumnNameToFieldName
    data.columns.flatMap(dfColumnNameToFieldNameMap.get)
  }

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
    val oldEntity = this.columns.headOption.flatMap(header.exprFor).map(_.key)
      .getOrElse(throw IllegalStateException("GraphScan table did not contain any fields"))

    val entityLabels: Set[String] = oldEntity.cypherType match {
      case CTNode(labels, _) => labels
      case CTRelationship(typ, _) => typ
      case _ => throw IllegalArgumentException("CTNode or CTRelationship", oldEntity.cypherType)
    }

    val renamedExprMapping = this.header.mappings.map { case (expr, aliases) =>
      expr -> expr.withOwner(v)
    }

    val withRenamedColumns = renamedExprMapping.foldLeft(data) {
      case (acc, (oldExpr, newExpr)) => acc.withColumnRenamed(oldExpr.columnName, newExpr.columnName)
    }

    val renamedExprs = renamedExprMapping.map(_._2)

    val relevantColumns = targetHeader.mappings.map { targetMapping =>
      val targetColName = targetMapping.columnName

      renamedExprs.find(_ == targetMapping.expr) match {
        case Some(sourceSlot) =>
          val sourceColName = ColumnName.of(sourceSlot)

          // the column exists in the source data
          if (sourceColName == targetColName)
            withRenamedColumns.col(targetColName)
          // the column exists in the source data but has a different data type (and thus different column name)
          else
            withRenamedColumns.col(sourceColName)
              .cast(targetMapping.expr.cypherType.getSparkType)
              .as(targetColName)

        case None =>
          val content = targetMapping.expr match {
            case HasLabel(_, label) if entityLabels.contains(label.name) => functions.lit(true)
            case _: HasLabel => functions.lit(false)
            case _: Type if entityLabels.size == 1 => functions.lit(entityLabels.head)
            case _ => functions.lit(null).cast(targetMapping.expr.cypherType.getSparkType)
          }
          content.as(targetColName)
      }
    }

    val withRelevantColumns = withRenamedColumns.select(relevantColumns.toSeq: _*)
    CAPSRecords.verifyAndCreate(targetHeader, withRelevantColumns)
  }

  //noinspection AccessorLikeMethodIsEmptyParen
  def toDF(): DataFrame = data

  override def toString: String = {
    val numRows = data.size
    if (header.mappings.isEmpty && numRows == 0) {
      s"CAPSRecords.empty"
    } else if (header.mappings.isEmpty && numRows == 1) {
      s"CAPSRecords.unit"
    } else {
      s"CAPSRecords($header, table with $numRows rows)"
    }
  }

}

object CAPSRecords extends CypherRecordsCompanion[CAPSRecords, CAPSSession] {

  // Used for Expressions that are not yet tied to a variable
  private[spark] val placeHolderVarName = ""

  // TODO: remove unneeded create functions (they seem redundant with Spark DataFrame construction)
  def create[A <: Product : TypeTag](columns: Seq[String], data: Seq[A])(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(data).toDF(columns: _*)))

  def create[A <: Product : TypeTag](data: Seq[A])(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(data)))

  def create(columns: String*)(rows: java.util.List[Row], schema: StructType)(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(rows, schema).toDF(columns: _*)))

  def create(rows: java.util.List[Row], schema: StructType)(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(rows, schema)))

  def create(columns: Seq[String], data: java.util.List[_], beanClass: Class[_])(
    implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(data, beanClass).toDF(columns: _*)))

  def create(data: java.util.List[_], beanClass: Class[_])(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(data, beanClass)))

  def create[A <: Product : TypeTag](rdd: RDD[A])(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(rdd)))

  def create(rowRDD: RDD[Row], schema: StructType)(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(rowRDD, schema)))

  def create(rowRDD: JavaRDD[Row], schema: StructType)(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(rowRDD, schema)))

  def create(rdd: RDD[_], beanClass: Class[_])(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(rdd, beanClass)))

  def create(rdd: JavaRDD[_], beanClass: Class[_])(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(caps.sparkSession.createDataFrame(rdd, beanClass)))

  def create(entityTable: CAPSEntityTable)(implicit caps: CAPSSession): CAPSRecords = {
    def sourceColumnToPropertyExpressionMapping(variable: Var): Map[String, Expr] = {
      val keyMap = entityTable.mapping.propertyMapping.map {
        case (key, sourceColumn) => sourceColumn -> Property(variable, PropertyKey(key))()
      }.foldLeft(Map.empty[String, Expr]) {
        case (m, (sourceColumn, propertyExpr)) =>
          if (m.contains(sourceColumn))
            throw DuplicateSourceColumnException(sourceColumn, variable)
          else m.updated(sourceColumn, propertyExpr)
      }
      val sourceKey = entityTable.mapping.sourceIdKey
      if (keyMap.contains(sourceKey))
        throw DuplicateSourceColumnException(sourceKey, variable)
      else keyMap.updated(sourceKey, variable)
    }

    // Computes map of sourceColumn -> Expression for nodes
    def sourceColumnNodeToExpressionMapping(nodeMapping: NodeMapping): Map[String, Expr] = {
      // TODO: Generate var. Nice-to-have property: Same DF gets same var.
      // TODO: Labels on node var?
      val generatedVar = Var(placeHolderVarName)(nodeMapping.cypherType)

      val entityMappings = sourceColumnToPropertyExpressionMapping(generatedVar)

      nodeMapping.optionalLabelMapping.map {
        case (label, sourceColumn) => {
          sourceColumn -> HasLabel(generatedVar, Label(label))(CTBoolean)
        }
      }.foldLeft(entityMappings) {
        case (m, (sourceColumn, expr)) =>
          if (m.contains(sourceColumn))
            throw DuplicateSourceColumnException(sourceColumn, generatedVar)
          else m.updated(sourceColumn, expr)
      }
    }

    // Computes map of sourceColumn -> Expression for relationships
    def sourceColumnRelationshipToExpressionMapping(relMapping: RelationshipMapping): Map[String, Expr] = {
      // TODO: Generate var. Nice-to-have property: Same DF gets same var.
      // TODO: Labels on node var?
      val relVar = Var(placeHolderVarName)(relMapping.cypherType)
      val entityMappings = sourceColumnToPropertyExpressionMapping(relVar)

      val sourceColumnToExpressionMapping: Map[String, Expr] = Seq(
        relMapping.sourceStartNodeKey -> StartNode(relVar)(CTInteger),
        relMapping.sourceEndNodeKey -> EndNode(relVar)(CTInteger)
      ).foldLeft(entityMappings) {
        case (acc, (slot, expr)) =>
          if (acc.contains(slot))
            throw DuplicateSourceColumnException(slot, relVar)
          else acc.updated(slot, expr)
      }
      relMapping.relTypeOrSourceRelTypeKey match {
        case Right((sourceRelTypeColumn, _)) if sourceColumnToExpressionMapping.contains(sourceRelTypeColumn) =>
          throw DuplicateSourceColumnException(sourceRelTypeColumn, relVar)
        case Right((sourceRelTypeColumn, _)) => sourceColumnToExpressionMapping.updated(sourceRelTypeColumn, Type(relVar)(CTString))
        case Left(_) => sourceColumnToExpressionMapping
      }
    }

    val sourceColumnToExpressionMap = entityTable match {
      case nt: CAPSNodeTable => sourceColumnNodeToExpressionMapping(nt.mapping)
      case rt: CAPSRelationshipTable => sourceColumnRelationshipToExpressionMapping(rt.mapping)
    }

    val (sourceHeader, sourceDataFrame) = {
      prepareDataFrame(entityTable.table.df)
    }

    // Remove columns from source data that are not contained in the mapping.
    // TODO: Map source columns instead of sourceHeader.slots?
    val updatedExprs = sourceHeader.map {
      case sourceMapping@(expr, aliases) =>
        def sourceColumnName = expr.columnName

        val targetExpression: Option[Expr] = sourceColumnToExpressionMap.get(sourceColumnName)
        targetExpression.map {
          case v: Var => v
          // TODO: Why are property types changed to the base table type?
          case p: Property => p.copy()(cypherType = expr.cypherType)
          case _ => expr
        }.getOrElse(expr)

      case slot =>
        slot.expr
    }
    val newHeader = RecordHeader(updatedExprs.toSeq: _*)
    val renamed = sourceDataFrame.toDF(newHeader.columns.toSeq: _*)

    CAPSRecords.createInternal(newHeader, renamed)
  }

  /**
    * Wraps a Spark SQL table (DataFrame) in a CAPSRecords, making it understandable by Cypher.
    *
    * @param df   table to wrap.
    * @param caps session to which the resulting CAPSRecords is tied.
    * @return a Cypher table.
    */
  private[spark] def wrap(df: DataFrame)(implicit caps: CAPSSession): CAPSRecords =
    verifyAndCreate(prepareDataFrame(df))

  /**
    * Validates the data types within the DataFrame for compatibility, creates an initial [[RecordHeader]] and aligns
    * the data frame column names according to that header.
    *
    * @param initialDataFrame initial data frame containing source data
    * @param caps             caps session
    * @return record header and according data frame
    */
  private def prepareDataFrame(initialDataFrame: DataFrame)(implicit caps: CAPSSession): (RecordHeader, DataFrame) = {
    val withCompatibleTypes = generalizeColumnTypes(initialDataFrame)
    val initialHeader = table.CAPSRecordHeader.fromSparkStructType(withCompatibleTypes.schema)
    val withRenamedColumns = withCompatibleTypes.toDF(initialHeader.columns.toSeq: _*)
    (initialHeader, withRenamedColumns)
  }

  /**
    * Normalises the dataframe by lifting numeric fields to Long and similar ops.
    */
  private def generalizeColumnTypes(initialDataFrame: DataFrame): DataFrame = {
    val toCast = initialDataFrame.schema.fields.filter(f => f.dataType.toCypherType(f.nullable).isEmpty)
    val dfWithCompatibleTypes: DataFrame = toCast.foldLeft(initialDataFrame) {
      case (df, field) =>
        val castType = field.dataType.cypherCompatibleDataType.getOrElse(
          throw IllegalArgumentException(
            s"a Spark type supported by Cypher: ${supportedTypes.mkString("[", ", ", "]")}",
            s"type ${field.dataType} of field $field"))
        df.mapColumn(field.name)(_.cast(castType))
    }
    dfWithCompatibleTypes
  }

  /**
    * This does not mandate that the <i>order</i> of the RecordHeader and the DataFrame are aligned, as long as the
    * <i>names</i> are the same (and not duplicated).
    *
    * @param headerAndData header and records
    * @param caps          CAPS session
    * @return CAPSRecords representing the input
    */
  def verifyAndCreate(headerAndData: (RecordHeader, DataFrame))(implicit caps: CAPSSession): CAPSRecords = {
    verifyAndCreate(headerAndData._1, headerAndData._2)
  }

  /**
    * This does not mandate that the <i>order</i> of the RecordHeader and the DataFrame are aligned, as long as the
    * <i>names</i> are the same (and not duplicated).
    *
    * @param initialHeader header of the records
    * @param initialData   data of the records
    * @param caps          CAPS session
    * @return CAPSRecords representing the input
    */
  def verifyAndCreate(initialHeader: RecordHeader, initialData: DataFrame)(implicit caps: CAPSSession): CAPSRecords = {
    if (initialData.sparkSession != caps.sparkSession) {
      throw IllegalArgumentException(
        "a DataFrame belonging to the same Spark session",
        "DataFrame from different session")
    }

    // Ensure no duplicate columns in initialData
    val initialDataColumns = initialData.columns

    val duplicateColumns = initialDataColumns.groupBy(identity).collect {
      case (key, values) if values.size > 1 => key
    }

    if (duplicateColumns.nonEmpty)
      throw IllegalArgumentException(
        "a DataFrame with distinct columns",
        s"a DataFrame with duplicate columns: $duplicateColumns")

    // Verify that all header column names exist in the data
    val headerColumnNames = initialHeader.columns
    val missingColumnNames = headerColumnNames -- initialDataColumns
    if (missingColumnNames.nonEmpty) {
      throw IllegalArgumentException(
        s"data with columns ${initialHeader.columns.toSeq.sorted.mkString("\n", ", ", "\n")}",
        s"data with missing columns ${missingColumnNames.toSeq.sorted.mkString("\n", ", ", "\n")}"
      )
    }

    // Verify column types
    initialHeader.mappings.foreach { exprMapping =>
      val dfSchema = initialData.schema
      val field = dfSchema(exprMapping.columnName)
      val cypherType = field.dataType.toCypherType(field.nullable)
        .getOrElse(throw IllegalArgumentException("a supported Spark type", field.dataType))
      val headerType = exprMapping.expr.cypherType
      // if the type in the data doesn't correspond to the type in the header we fail
      // except: we encode nodes, rels and integers with the same data type, so we can't fail
      // on conflicts when we expect entities (alternative: change reverse-mapping function somehow)
      if (headerType.toSparkType != cypherType.toSparkType && !containsEntity(headerType))
        throw IllegalArgumentException(s"a valid data type for column ${field.name} of type $headerType", cypherType)
    }
    createInternal(initialHeader, initialData)
  }

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit caps: CAPSSession): CAPSRecords = {
    val initialSparkStructType = StructType(initialHeader.columnMappings.values.toSeq)
    val initialDataFrame = caps.sparkSession.createDataFrame(Collections.emptyList[Row](), initialSparkStructType)
    createInternal(initialHeader, initialDataFrame)
  }

  override def unit()(implicit caps: CAPSSession): CAPSRecords = {
    val initialDataFrame = caps.sparkSession.createDataFrame(Seq(EmptyRow()))
    createInternal(RecordHeader.empty, initialDataFrame)
  }

  private def createInternal(header: RecordHeader, data: DataFrame)(implicit caps: CAPSSession) =
    new CAPSRecords(header, data) {}

  @tailrec
  private def containsEntity(t: CypherType): Boolean = t match {
    case _: CTNode => true
    case _: CTRelationship => true
    case l: CTList => containsEntity(l.elementType)
    case _ => false
  }

  private case class EmptyRow()

}
