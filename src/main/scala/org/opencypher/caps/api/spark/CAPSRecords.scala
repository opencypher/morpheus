/**
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
package org.opencypher.caps.api.spark

import java.io.PrintStream
import java.util.Collections

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.opencypher.caps.api.expr.{Property, Var}
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.util.PrintOptions
import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.impl.record.CAPSRecordHeader
import org.opencypher.caps.impl.spark.convert.{fromSparkType, rowToCypherMap, toSparkType}
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.spark.{RecordsPrinter, SparkColumnName}
import org.opencypher.caps.impl.syntax.header._

import scala.annotation.tailrec
import scala.reflect.runtime.universe.TypeTag

sealed abstract class CAPSRecords(
  initialHeader: RecordHeader,
  initialData: DataFrame,
  optDetailedRecords: Option[CAPSRecords]
)(implicit val caps: CAPSSession)
  extends CypherRecords with Serializable {

  self =>

  override type Data = DataFrame
  override type Records = CAPSRecords

  override def header: RecordHeader = initialHeader
  override def data: DataFrame = initialData

  override val fields: Set[String] = header.fields.map(_.name)
  override val fieldsInOrder: Seq[String] = header.fieldsInOrder.map(_.name)

  def sparkColumns: IndexedSeq[String] = header.internalHeader.columns

  //noinspection AccessorLikeMethodIsEmptyParen
  def toDF(): Data = data

  def mapDF(f: Data => Data): CAPSRecords = CAPSRecords.create(f(data))

  // TODO: Check that this does not change the caching of our data frame
  def cached: CAPSRecords = CAPSRecords.create(header, data.cache())

  override def print(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  def details: CAPSRecords = optDetailedRecords.getOrElse(this)

  def compact: CAPSRecords = {
    val cachedHeader = self.header.update(compactFields)._1
    val cachedData = {
      val columns = cachedHeader.slots.map(c => new Column(SparkColumnName.of(c.content)))
      self.data.select(columns: _*)
    }

    CAPSRecords.create(cachedHeader, cachedData)
  }

  def unionAll(header: RecordHeader, other: CAPSRecords): CAPSRecords = {
    val unionData = details.data.union(other.details.data)
    CAPSRecords.create(header, unionData)
  }

  override def contract[E <: EmbeddedEntity](entity: VerifiedEmbeddedEntity[E]): CAPSRecords = {
    val slotExprs = entity.slots
    val newSlots = header.slots.map {
      case slot@RecordSlot(idx, content: FieldSlotContent) =>
        slotExprs.get(content.field.name).map {
          case expr: Var      => OpaqueField(expr)
          case expr: Property => ProjectedExpr(expr.copy()(content.cypherType))
          case expr           => ProjectedExpr(expr)
        }
        .getOrElse(slot.content)

      case slot =>
        slot.content
    }
    val newHeader = RecordHeader.from(newSlots: _*)
    val renamed = data.toDF(newHeader.internalHeader.columns: _*)
    CAPSRecords.create(newHeader, renamed)
  }

  def distinct: CAPSRecords = {
    CAPSRecords.create(self.header, self.details.data.distinct())
  }

  def toLocalScalaIterator: Iterator[CypherMap] = {
    import scala.collection.JavaConverters._

    toLocalIterator.asScala
  }

  def toLocalIterator: java.util.Iterator[CypherMap] =
    toCypherMaps.toLocalIterator()

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
}

object CAPSRecords {

  def create[A <: Product : TypeTag](columns: Seq[String], data: Seq[A])(implicit caps: CAPSSession)
  : CAPSRecords =
    create(caps.sparkSession.createDataFrame(data).toDF(columns: _*))

  def create[A <: Product : TypeTag](data: Seq[A])(implicit caps: CAPSSession)
  : CAPSRecords =
    create(caps.sparkSession.createDataFrame(data))

  def create(columns: String*)(rows: java.util.List[Row], schema: StructType)(implicit caps: CAPSSession)
  : CAPSRecords =
    create(caps.sparkSession.createDataFrame(rows, schema).toDF(columns: _*))

  def create(rows: java.util.List[Row], schema: StructType)(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(rows, schema))

  def create(columns: Seq[String], data: java.util.List[_], beanClass: Class[_])(implicit caps: CAPSSession)
  : CAPSRecords =
    create(caps.sparkSession.createDataFrame(data, beanClass).toDF(columns: _*))

  def create(data: java.util.List[_], beanClass: Class[_])(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(data, beanClass))

  def create[A <: Product : TypeTag](rdd: RDD[A])(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(rdd))

  def create(rowRDD: RDD[Row], schema: StructType)(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(rowRDD, schema))

  def create(rowRDD: JavaRDD[Row], schema: StructType)(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(rowRDD, schema))

  def create(rdd: RDD[_], beanClass: Class[_])(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(rdd, beanClass))

  def create(rdd: JavaRDD[_], beanClass: Class[_])(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(rdd, beanClass))

  def create(initialDataFrame: DataFrame)(implicit caps: CAPSSession): CAPSRecords = {
    val initialHeader = CAPSRecordHeader.fromSparkStructType(initialDataFrame.schema)

    // rename data to match generated header
    // we trust the order of the generated header
    val renamed = initialDataFrame.toDF(initialHeader.internalHeader.columns: _*)

    create(initialHeader, renamed)
  }

  /**
    * This does not mandate that the <i>order</i> of the RecordHeader and the DataFrame are aligned, as long as the
    * <i>names</i> are the same (and not duplicated).
    *
    * @param initialHeader the header of the records.
    * @param initialData the data of the records.
    * @param caps the space in which the data belongs.
    * @return a new SparkCypherRecords representing the input.
    */
  def create(initialHeader: RecordHeader, initialData: DataFrame)(implicit caps: CAPSSession)
  : CAPSRecords = {
    if (initialData.sparkSession == caps.sparkSession) {

      // Ensure no duplicate columns in initialData
      val initialDataColumns = initialData.columns.toSeq
      if (initialDataColumns.size != initialDataColumns.distinct.size)
        Raise.duplicateColumnNamesInData()

      // Verify that all header column names exist in the data
      val headerColumnNames = initialHeader.internalHeader.columns.toSet
      val dataColumnNames = initialData.columns.toSet
      if (!headerColumnNames.subsetOf(dataColumnNames)) {
        Raise.recordsDataHeaderMismatch(
          s"with columns ${initialHeader.internalHeader.columns.toSet.mkString(", ")}",
          s"with columns ${initialData.columns.toSet.mkString(", ")}"
        )
      }

      // Verify column types
      initialHeader.slots.foreach { slot =>
        val dfSchema = initialData.schema
        val field = dfSchema(SparkColumnName.of(slot))
        val cypherType = fromSparkType(field.dataType, field.nullable)
        val headerType = slot.content.cypherType

        // if the type in the data doesn't correspond to the type in the header we fail
        // except: we encode nodes, rels and integers with the same data type, so we can't fail
        // on conflicts when we expect entities (alternative: change reverse-mapping function somehow)
        if (toSparkType(headerType) != toSparkType(cypherType) && !containsEntity(headerType))
          Raise.invalidDataTypeForColumn(field.name, headerType.toString, cypherType.toString)
      }

      val internalRecords = createInternal(initialHeader, initialData, None)
      val isSanitized = initialHeader.slots.map(_.content).collectFirst { case _: ProjectedExpr => true }.isEmpty
      if (isSanitized) {
        internalRecords
      } else {
        val fieldContents = initialHeader.contents.collect { case content: FieldSlotContent => content }.toSeq
        val (sanitizedHeader, _) = RecordHeader.empty.update(addContents(fieldContents))
        val remainingColumnNames = sanitizedHeader.slots.map { s => SparkColumnName.of(s.content) }.toSet
        val existingColumns = initialData.columns
        val sanitizedColumns = existingColumns.filter(remainingColumnNames).map(initialData.col)
        val sanitizedData = initialData.select(sanitizedColumns: _*)
        createInternal(sanitizedHeader, sanitizedData, Some(internalRecords))
      }
    }
    else {
      Raise.capsSessionMismatch()
    }
  }

  private def createInternal(header: RecordHeader, data: DataFrame, optRecordsWithDetails: Option[CAPSRecords])
                            (implicit caps: CAPSSession) =
    new CAPSRecords(header, data, optRecordsWithDetails) {}

  @tailrec
  private def containsEntity(t: CypherType): Boolean = t match {
    case _: CTNode => true
    case _: CTRelationship => true
    case l: CTList => containsEntity(l.elementType)
    case _ => false
  }

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit caps: CAPSSession)
  : CAPSRecords = {
    val initialSparkStructType = CAPSRecordHeader.asSparkStructType(initialHeader)
    val initialDataFrame = caps.sparkSession.createDataFrame(Collections.emptyList[Row](), initialSparkStructType)
    create(initialHeader, initialDataFrame)
  }

  def unit()(implicit caps: CAPSSession): CAPSRecords = {
    val initialDataFrame = caps.sparkSession.createDataFrame(Seq(EmptyRow()))
    create(RecordHeader.empty, initialDataFrame)
  }

  private case class EmptyRow()
}
