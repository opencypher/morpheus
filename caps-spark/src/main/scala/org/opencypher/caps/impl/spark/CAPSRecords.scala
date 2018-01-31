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
package org.opencypher.caps.impl.spark

import java.util.Collections

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.{DuplicateSourceColumnException, IllegalArgumentException, IllegalStateException}
import org.opencypher.caps.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.{EntityTable, NodeTable, RelationshipTable}
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value._
import org.opencypher.caps.impl.record.CAPSRecordHeader._
import org.opencypher.caps.impl.record.{CAPSRecordHeader, _}
import org.opencypher.caps.impl.spark.CAPSRecords.{prepareDataFrame, verifyAndCreate}
import org.opencypher.caps.impl.spark.DfUtils._
import org.opencypher.caps.impl.spark.convert.SparkUtils._
import org.opencypher.caps.impl.spark.convert.rowToCypherMap
import org.opencypher.caps.impl.syntax.RecordHeaderSyntax._
import org.opencypher.caps.impl.util.PrintOptions
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.api.{Label, PropertyKey}

import scala.annotation.tailrec
import scala.reflect.runtime.universe.TypeTag

sealed abstract class CAPSRecords(override val header: RecordHeader, val data: DataFrame)
  (implicit val caps: CAPSSession) extends CypherRecords with Serializable {

  override def print(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  override def size: Long = data.count()

  def sparkColumns: IndexedSeq[String] = header.internalHeader.columns

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

  def select(fields: Set[Var]): CAPSRecords = {
    val selectedHeader = header.select(fields)
    val selectedColumnNames = selectedHeader.contents.map(SparkColumnName.of).toSeq
    val selectedColumns = data.select(selectedColumnNames.head, selectedColumnNames.tail: _*)
    CAPSRecords.verifyAndCreate(selectedHeader, selectedColumns)
  }

  def compact(implicit details: RetainedDetails): CAPSRecords = {
    val cachedHeader = header.update(compactFields)._1
    if (header == cachedHeader) {
      this
    } else {
      val cachedData = {
        val columns = cachedHeader.slots.map(c => new Column(SparkColumnName.of(c.content)))
        data.select(columns: _*)
      }

      CAPSRecords.verifyAndCreate(cachedHeader, cachedData)
    }
  }

  def unionAll(header: RecordHeader, other: CAPSRecords): CAPSRecords = {
    val unionData = data.union(other.data)
    CAPSRecords.verifyAndCreate(header, unionData)
  }

  def distinct: CAPSRecords = {
    CAPSRecords.verifyAndCreate(header, data.distinct())
  }

  /**
    * Converts all values stored in this table to instances of the corresponding CypherValue class.
    * In particular, this de-flattens, or collects, flattened entities (nodes and relationships) into
    * compact CypherNode/CypherRelationship objects.
    *
    * All values on each row are inserted into a CypherMap object mapped to the corresponding field name.
    *
    * @return a dataset of CypherMaps.
    */
  def toCypherMaps: Dataset[CAPSMap] = {
    import encoders._
    data.map(rowToCypherMap(header))
  }

  override def iterator: Iterator[CAPSMap] = {
    import scala.collection.JavaConverters._

    toLocalIterator.asScala
  }

  def toLocalIterator: java.util.Iterator[CAPSMap] = {
    toCypherMaps.toLocalIterator()
  }

  def foreachPartition(f: Iterator[CAPSMap] => Unit): Unit = {
    toCypherMaps.foreachPartition(f)
  }

  def collect(): Array[CAPSMap] =
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
    val oldEntity = this.header.internalHeader.fields.headOption
      .getOrElse(throw IllegalStateException("GraphScan table did not contain any fields"))
    val entityLabels: Set[String] = oldEntity.cypherType match {
      case CTNode(labels) => labels
      case CTRelationship(typ) => typ
      case _ => throw IllegalArgumentException("CTNode or CTRelationship", oldEntity.cypherType)
    }

    val slots = this.header.slots
    val renamedSlots = slots.map(_.withOwner(v))

    val dataColumnNameToIndex: Map[String, Int] = renamedSlots.map { dataSlot =>
      val dataColumnName = SparkColumnName.of(dataSlot)
      val dataColumnIndex = dataSlot.index
      dataColumnName -> dataColumnIndex
    }.toMap

    val slotDataSelectors: Seq[Row => Any] = targetHeader.slots.map { targetSlot =>
      val columnName = SparkColumnName.of(targetSlot)
      val defaultValue = targetSlot.content.key match {
        case HasLabel(_, l: Label) => entityLabels(l.name)
        case _: Type if entityLabels.size == 1 => entityLabels.head
        case _ => null
      }
      val maybeDataIndex = dataColumnNameToIndex.get(columnName)
      val slotDataSelector: Row => Any = maybeDataIndex match {
        case None =>
          (_) =>
            defaultValue
        case Some(index) => _.get(index)
      }
      slotDataSelector
    }
    val wrappedHeader = new CAPSRecordHeader(targetHeader)

    val alignedData = this
      .toDF()
      .map { (row: Row) =>
        val alignedRow = slotDataSelectors.map(_ (row))
        new GenericRowWithSchema(alignedRow.toArray, wrappedHeader.asSparkSchema).asInstanceOf[Row]
      }(wrappedHeader.rowEncoder)

    CAPSRecords.verifyAndCreate(targetHeader, alignedData)
  }

  //noinspection AccessorLikeMethodIsEmptyParen
  def toDF(): DataFrame = data
}

object CAPSRecords {

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

  def create(entityTable: EntityTable)(implicit caps: CAPSSession): CAPSRecords = {
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
      case nt: NodeTable => sourceColumnNodeToExpressionMapping(nt.mapping)
      case rt: RelationshipTable => sourceColumnRelationshipToExpressionMapping(rt.mapping)
    }

    val (sourceHeader, sourceDataFrame) = prepareDataFrame(entityTable.table)

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
    val newHeader = RecordHeader.from(slotContents: _*)
    val renamed = sourceDataFrame.toDF(newHeader.internalHeader.columns: _*)

    CAPSRecords.verifyAndCreate(newHeader, renamed)
  }

  /**
    * Validates the data types within the DataFrame for compatibility, creates an initial [[RecordHeader]] and upgraded
    * the data frame column names according to that header.
    *
    * @param initialDataFrame initial data frame containing source data
    * @param caps             caps session
    * @return record header and according data frame
    */
  private def prepareDataFrame(initialDataFrame: DataFrame)(implicit caps: CAPSSession): (RecordHeader, DataFrame) = {
    val withCompatibleTypes = generalizeColumnTypes(initialDataFrame)
    val initialHeader = CAPSRecordHeader.fromSparkStructType(withCompatibleTypes.schema)
    val withRenamedColumns = withCompatibleTypes.toDF(initialHeader.internalHeader.columns: _*)
    (initialHeader, withRenamedColumns)
  }

  private def generalizeColumnTypes(initialDataFrame: DataFrame): DataFrame = {
    val toCast = initialDataFrame.schema.fields.filter(f => fromSparkType(f.dataType, f.nullable).isEmpty)
    val dfWithCompatibleTypes: DataFrame = toCast.foldLeft(initialDataFrame) {
      case (df, field) =>
        val castType = cypherCompatibleDataType(field.dataType).getOrElse(
          throw IllegalArgumentException("a Spark type supported by Cypher", s"type ${field.dataType} of field $field"))
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
    val initialDataColumns = initialData.columns.toSeq

    val duplicateColumns = initialDataColumns.groupBy(identity).collect {
      case (key, values) if values.size > 1 => key
    }

    if (duplicateColumns.nonEmpty)
      throw IllegalArgumentException(
        "a DataFrame with distinct columns",
        s"a DataFrame with duplicate columns: $duplicateColumns")

    // Verify that all header column names exist in the data
    val headerColumnNames = initialHeader.internalHeader.columns.toSet
    val dataColumnNames = initialData.columns.toSet
    val missingColumnNames = headerColumnNames -- dataColumnNames
    if (missingColumnNames.nonEmpty) {
      throw IllegalArgumentException(
        s"data with columns ${initialHeader.internalHeader.columns.sorted.mkString("\n", ", ", "\n")}",
        s"data with missing columns ${missingColumnNames.toSeq.sorted.mkString("\n", ", ", "\n")}"
      )
    }

    // Verify column types
    initialHeader.slots.foreach { slot =>
      val dfSchema = initialData.schema
      val field = dfSchema(SparkColumnName.of(slot))
      val cypherType = fromSparkType(field.dataType, field.nullable)
        .getOrElse(throw IllegalArgumentException("a supported Spark type", field.dataType))
      val headerType = slot.content.cypherType

      // if the type in the data doesn't correspond to the type in the header we fail
      // except: we encode nodes, rels and integers with the same data type, so we can't fail
      // on conflicts when we expect entities (alternative: change reverse-mapping function somehow)
      if (toSparkType(headerType) != toSparkType(cypherType) && !containsEntity(headerType))
        throw IllegalArgumentException(s"a valid data type for column ${field.name} of type $headerType", cypherType)
    }
    createInternal(initialHeader, initialData)
  }

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit caps: CAPSSession): CAPSRecords = {
    val initialSparkStructType = CAPSRecordHeader.asSparkStructType(initialHeader)
    val initialDataFrame = caps.sparkSession.createDataFrame(Collections.emptyList[Row](), initialSparkStructType)
    verifyAndCreate(initialHeader, initialDataFrame)
  }

  def unit()(implicit caps: CAPSSession): CAPSRecords = {
    val initialDataFrame = caps.sparkSession.createDataFrame(Seq(EmptyRow()))
    verifyAndCreate(RecordHeader.empty, initialDataFrame)
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
