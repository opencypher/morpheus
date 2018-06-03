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

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.table.{CypherRecords, CypherRecordsCompanion}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, UnsupportedOperationException}
import org.opencypher.okapi.impl.table._
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.impl.exception.DuplicateSourceColumnException
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.EntityTable._
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.CAPSRecords.{prepareDataFrame, verifyAndCreate}
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.convert.CAPSCypherType._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag

sealed abstract case class CAPSRecords(header: RecordHeaderNew, data: DataFrame)
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
    val idColumns = header.idColumns
    val dfWithReplacedTags = idColumns.foldLeft(data) {
      case (df, column) => df.safeReplaceTags(column, actualRetaggings)
    }

    CAPSRecords.verifyAndCreate(header, dfWithReplacedTags)
  }

  def select(fields: Set[Var]): CAPSRecords = {
    val selectedHeader = header.select(fields)
    val selectedColumnNames = selectedHeader.columns.toSeq
    val selectedColumns = data.select(selectedColumnNames.head, selectedColumnNames.tail: _*)
    CAPSRecords.verifyAndCreate(selectedHeader, selectedColumns)
  }

//  def compact(implicit details: RetainedDetails): CAPSRecords = {
//    val cachedHeader = header.update(compactFields)._1
//    if (header == cachedHeader) {
//      this
//    } else {
//      val cachedData = {
//        val columns = cachedHeader.slots.map(c => new Column(cachedHeader.of(c.content)))
//        data.select(columns: _*)
//      }
//
//      CAPSRecords.verifyAndCreate(cachedHeader, cachedData)
//    }
//  }

  def addAliases(aliasToOriginal: Map[Var, Var]): CAPSRecords = {
    val (updatedHeader, updatedData) = aliasToOriginal.foldLeft((header, data)) {
      case ((tempHeader, tempDf), (nextAlias, nextOriginal)) =>

        val updatedHeader = tempHeader.withAlias(nextAlias, nextOriginal)

        val aliasedExpressions = tempHeader
          .ownedBy(nextOriginal)
          .map(expr => expr -> expr.withOwner(nextAlias))

        val additions = aliasedExpressions.map {
          case (expr, alias) => updatedHeader.column(alias) -> tempDf.col(updatedHeader.column(expr))
        }
        updatedHeader -> tempDf.safeAddColumns(additions.toSeq: _*)
    }
    CAPSRecords.verifyAndCreate(updatedHeader, updatedData)
  }

  def retagVariable(v: Var, replacements: Map[Int, Int]): CAPSRecords = {
    val columnsToUpdate = header.idColumns(v)
    val updatedData = columnsToUpdate.foldLeft(data) { case (df, columnName) =>
      df.safeReplaceTags(columnName, replacements)
    }
    CAPSRecords.verifyAndCreate(header, updatedData)
  }

  def renameVars(aliasToOriginal: Map[Var, Var]): CAPSRecords = {
    val (updatedHeader, updatedData) = aliasToOriginal.foldLeft((header, data)) {
      case ((tempHeader, tempDf), (nextAlias, nextOriginal)) =>
        val slotsToReassign = tempHeader.selfWithChildren(nextOriginal).toList
        val reassignedSlots = slotsToReassign.map(_.withOwner(nextAlias))
        val updatedHeader = tempHeader --
          IRecordHeader.from(slotsToReassign) ++
          IRecordHeader.from(reassignedSlots)
        val originalColumns = slotsToReassign.map(header.of)
        val aliasColumns = reassignedSlots.map(header.of)
        val renamings = originalColumns.zip(aliasColumns)
        val updatedDf = tempDf.safeRenameColumns(renamings: _*)
        updatedHeader -> updatedDf
    }
    CAPSRecords.verifyAndCreate(updatedHeader, updatedData)
  }

  def removeVars(vars: Set[Var]): CAPSRecords = {
    val (updatedHeader, updatedData) = vars.foldLeft((header, data)) {
      case ((tempHeader, tempDf), nextFieldToRemove) =>
        val slotsToRemove = tempHeader.selfWithChildren(nextFieldToRemove)
        val updatedHeader = tempHeader -- IRecordHeader.from(slotsToRemove.toList)
        updatedHeader -> tempDf.drop(slotsToRemove.map(tempHeader.of): _*)
    }
    CAPSRecords.verifyAndCreate(updatedHeader, updatedData)
  }

  def unionAll(header: IRecordHeader, other: CAPSRecords): CAPSRecords = {
    val unionData = data.union(other.data)
    CAPSRecords.verifyAndCreate(header, unionData)
  }

  def distinct: CAPSRecords = {
    CAPSRecords.verifyAndCreate(header, data.distinct())
  }

  def distinct(fields: Var*): CAPSRecords =
    CAPSRecords.verifyAndCreate(header, data.dropDuplicates(fields.map(OpaqueField).map(header.of)))

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

  override def columns: Seq[String] = header.fieldsInOrder

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
  def alignWith(v: Var, targetHeader: IRecordHeader): CAPSRecords = {
    val oldEntity = this.header.fieldsAsVar.headOption
      .getOrElse(throw IllegalStateException("GraphScan table did not contain any fields"))

    val entityLabels: Set[String] = oldEntity.cypherType match {
      case CTNode(labels, _) => labels
      case CTRelationship(typ, _) => typ
      case _ => throw IllegalArgumentException("CTNode or CTRelationship", oldEntity.cypherType)
    }

    val renamedSlotMapping = this.header.slots.map { slot =>
      val withNewOwner = slot.withOwner(v).content
      slot -> targetHeader.slots.find(slot => slot.content == withNewOwner).get
    }

    val withRenamedColumns = renamedSlotMapping.foldLeft(data) {
      case (acc, (oldCol, newCol)) =>
        val oldColName = header.of(oldCol)
        val newColName = targetHeader.of(newCol)
        if (oldColName != newColName) {
          acc
            .safeReplaceColumn(oldColName, acc.col(oldColName).cast(newCol.content.cypherType.toSparkType.get))
            .safeRenameColumn(oldColName, newColName)
        } else {
          acc
        }
    }

    val renamedSlots = renamedSlotMapping.map(_._2)

    def typeAlignmentError(alignWithSlot: RecordSlot, varToAlign: Var, typeToAlign: CypherType) = {
      val varToAlignName = varToAlign.name
      val varToAlignString = if (varToAlignName.isEmpty) "table" else s"variable '$varToAlignName'"

      throw UnsupportedOperationException(
        s"""|Cannot align $varToAlignString with '${v.name}' due the alignment target type for ${alignWithSlot.content.key.withoutType}:
            |  The target type on '${v.name}' is ${alignWithSlot.content.cypherType}, whilst the $varToAlignString type is $typeToAlign""".stripMargin)
    }

    val relevantColumns = targetHeader.slots.map { targetSlot =>
      val targetColName = targetHeader.of(targetSlot)

      renamedSlots.find(_.content == targetSlot.content) match {
        case Some(sourceSlot) =>
          val sourceColName = targetHeader.of(sourceSlot)

          // the column exists in the source data
          if (sourceColName == targetColName) {
            withRenamedColumns.col(targetColName)
            // the column exists in the source data but has a different data type (and thus different column name)
          } else {
            val slotType = targetSlot.content.cypherType
            val sparkTypeOpt = slotType.toSparkType
            sparkTypeOpt match {
              case Some(sparkType) =>
                withRenamedColumns.col(sourceColName)
                  .cast(sparkType)
                  .as(targetColName)
              case None => typeAlignmentError(targetSlot, oldEntity, sourceSlot.content.cypherType)

            }
          }

        case None =>
          val content = targetSlot.content.key match {
            case HasLabel(_, label) if entityLabels.contains(label.name) => functions.lit(true)
            case _: HasLabel => functions.lit(false)
            case _: Type if entityLabels.size == 1 => functions.lit(entityLabels.head)
            case _ =>
              // TODO: This check cannot be enabled, because nullability of slot contents is often not correct in tests
//              if (targetSlot.content.cypherType.isNullable) {
//                throw UnsupportedOperationException(
//                  s"Cannot align scan on $v by adding a NULL column, because the type for '${targetSlot.content.key}' is non-nullable"
//                )
//              }
              functions.lit(null).cast(targetSlot.content.cypherType.getSparkType)
          }
          content.as(targetColName)
      }

    }

    val withRelevantColumns = withRenamedColumns.select(relevantColumns: _*)
    CAPSRecords.verifyAndCreate(targetHeader, withRelevantColumns)
  }

  //noinspection AccessorLikeMethodIsEmptyParen
  def toDF(): DataFrame = data

  override def toString: String = {
    val numRows = data.size
    if (header.slots.isEmpty && numRows == 0) {
      s"CAPSRecords.empty"
    } else if (header.slots.isEmpty && numRows == 1) {
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
    // Wrap source expressions in OpaqueField/ProjectedExpr
    // TODO: Map source columns instead of sourceHeader.slots?
    val slotContents: Seq[SlotContent] = sourceHeader.slots.map {
      case slot@RecordSlot(_, content: FieldSlotContent) =>
        def sourceColumnName = content.field.name

        val expressionForColumn = sourceColumnToExpressionMap.get(sourceColumnName)
        expressionForColumn.map {
          case expr: Var => OpaqueField(expr)
          case expr: Property => ProjectedExpr(expr.copy()(cypherType = content.cypherType))
          case expr => ProjectedExpr(expr)
        }.getOrElse(slot.content)

      case slot =>
        slot.content
    }
    val newHeader = IRecordHeader.from(slotContents: _*)
    val renamed = sourceDataFrame.toDF(newHeader.columns: _*)

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
    * Validates the data types within the DataFrame for compatibility, creates an initial [[IRecordHeader]] and aligns
    * the data frame column names according to that header.
    *
    * @param initialDataFrame initial data frame containing source data
    * @param caps             caps session
    * @return record header and according data frame
    */
  private def prepareDataFrame(initialDataFrame: DataFrame)(implicit caps: CAPSSession): (IRecordHeader, DataFrame) = {
    val withCompatibleTypes = generalizeColumnTypes(initialDataFrame)
    val initialHeader = table.CAPSRecordHeader.fromSparkStructType(withCompatibleTypes.schema)
    val withRenamedColumns = withCompatibleTypes.toDF(initialHeader.columns: _*)
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
  def verifyAndCreate(headerAndData: (IRecordHeader, DataFrame))(implicit caps: CAPSSession): CAPSRecords = {
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
  def verifyAndCreate(initialHeader: RecordHeaderNew, initialData: DataFrame)(implicit caps: CAPSSession): CAPSRecords = {
    if (initialData.sparkSession != caps.sparkSession) {
      throw IllegalArgumentException(
        "a DataFrame belonging to the same Spark session",
        "DataFrame from different session")
    }

    // Ensure no duplicate columns in initialData
    val initialDataColumns = initialData.columns.toSeq

    val duplicateColumns = initialDataColumns.groupBy(identity).collect {
      case (key, values) if values.size > 1 => key
    }

    if (duplicateColumns.nonEmpty)
      throw IllegalArgumentException(
        "a DataFrame with distinct columns",
        s"a DataFrame with duplicate columns: $duplicateColumns")

    // Verify that all header column names exist in the data
    val headerColumnNames = initialHeader.columns.toSet
    val dataColumnNames = initialData.columns.toSet
    val missingColumnNames = headerColumnNames -- dataColumnNames
    if (missingColumnNames.nonEmpty) {
      throw IllegalArgumentException(
        s"data with columns ${initialHeader.columns.sorted.mkString("\n", ", ", "\n")}",
        s"data with missing columns ${missingColumnNames.toSeq.sorted.mkString("\n", ", ", "\n")}"
      )
    }

    // Verify column types
    initialHeader.slots.foreach { slot =>
      val dfSchema = initialData.schema
      val field = dfSchema(initialHeader.of(slot))
      val cypherType = field.dataType.toCypherType(field.nullable)
        .getOrElse(throw IllegalArgumentException("a supported Spark type", field.dataType))
      val headerType = slot.content.cypherType
      // if the type in the data doesn't correspond to the type in the header we fail
      // except: we encode nodes, rels and integers with the same data type, so we can't fail
      // on conflicts when we expect entities (alternative: change reverse-mapping function somehow)
      if (headerType.toSparkType != cypherType.toSparkType && !containsEntity(headerType))
        throw IllegalArgumentException(s"a valid data type for column ${field.name} of type $headerType", cypherType)
    }
    createInternal(initialHeader, initialData)
  }

  def empty(initialHeader: IRecordHeader = IRecordHeader.empty)(implicit caps: CAPSSession): CAPSRecords = {
    val initialSparkStructType = initialHeader.toStructType
    val initialDataFrame = caps.sparkSession.createDataFrame(Collections.emptyList[Row](), initialSparkStructType)
    createInternal(initialHeader, initialDataFrame)
  }

  override def unit()(implicit caps: CAPSSession): CAPSRecords = {
    val initialDataFrame = caps.sparkSession.createDataFrame(Seq(EmptyRow()))
    createInternal(IRecordHeader.empty, initialDataFrame)
  }


  private def createInternal(header: IRecordHeader, data: DataFrame)(implicit caps: CAPSSession) =
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
