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
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CAPSMap
import org.opencypher.caps.impl.record.CAPSRecordHeader._
import org.opencypher.caps.impl.record.{CAPSRecordHeader, _}
import org.opencypher.caps.impl.spark.DfUtils._
import org.opencypher.caps.impl.spark.convert.{fromSparkType, rowToCypherMap, toSparkType}
import org.opencypher.caps.impl.syntax.RecordHeaderSyntax._
import org.opencypher.caps.impl.util.PrintOptions
import org.opencypher.caps.ir.api.expr.{Property, Var}

import scala.annotation.tailrec
import scala.reflect.runtime.universe.TypeTag

sealed abstract class CAPSRecords(
    override val header: RecordHeader,
    val data: DataFrame
)(implicit val caps: CAPSSession)
    extends CypherRecords
    with Serializable {

  override def print(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  override def iterator: Iterator[CAPSMap] = {
    import scala.collection.JavaConverters._

    toLocalIterator.asScala
  }

  override def size: Long = data.count()

  def sparkColumns: IndexedSeq[String] = header.internalHeader.columns

  //noinspection AccessorLikeMethodIsEmptyParen
  def toDF(): DataFrame = data

  def mapDF(f: DataFrame => DataFrame): CAPSRecords = CAPSRecords.create(f(data))

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
    CAPSRecords.create(selectedHeader, selectedColumns)
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

      CAPSRecords.create(cachedHeader, cachedData)
    }
  }

  def unionAll(header: RecordHeader, other: CAPSRecords): CAPSRecords = {
    val unionData = data.union(other.data)
    CAPSRecords.create(header, unionData)
  }

  def contract[E <: EmbeddedEntity](entity: VerifiedEmbeddedEntity[E]): CAPSRecords = {
    val slotExprs = entity.slots
    val newSlots = header.slots.map {
      case slot @ RecordSlot(idx, content: FieldSlotContent) =>
        slotExprs
          .get(content.field.name)
          .map {
            case expr: Var      => OpaqueField(expr)
            case expr: Property => ProjectedExpr(expr.copy()(cypherType = content.cypherType))
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
    CAPSRecords.create(header, data.distinct())
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
}

object CAPSRecords {

  def create[A <: Product: TypeTag](columns: Seq[String], data: Seq[A])(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(data).toDF(columns: _*))

  def create[A <: Product: TypeTag](data: Seq[A])(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(data))

  def create(columns: String*)(rows: java.util.List[Row], schema: StructType)(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(rows, schema).toDF(columns: _*))

  def create(rows: java.util.List[Row], schema: StructType)(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(rows, schema))

  def create(columns: Seq[String], data: java.util.List[_], beanClass: Class[_])(
      implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(data, beanClass).toDF(columns: _*))

  def create(data: java.util.List[_], beanClass: Class[_])(implicit caps: CAPSSession): CAPSRecords =
    create(caps.sparkSession.createDataFrame(data, beanClass))

  def create[A <: Product: TypeTag](rdd: RDD[A])(implicit caps: CAPSSession): CAPSRecords =
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
    val toCast = initialDataFrame.schema.fields.filter(f => fromSparkType(f.dataType, f.nullable).isEmpty)
    val dfWithCompatibleTypes: DataFrame = toCast.foldLeft(initialDataFrame) {
      case (df, field) =>
        val castType = field.dataType match {
          case ByteType | ShortType | IntegerType => LongType
          case FloatType                          => DoubleType
          case other =>
            throw IllegalArgumentException("a Spark type supported by Cypher", s"type $other of field $field")
        }
        df.mapColumn(field.name)(_.cast(castType))
    }

    val initialHeader = CAPSRecordHeader.fromSparkStructType(dfWithCompatibleTypes.schema)

    // rename data to match generated header
    // we trust the order of the generated header
    val renamed = dfWithCompatibleTypes.toDF(initialHeader.internalHeader.columns: _*)

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
  def create(initialHeader: RecordHeader, initialData: DataFrame)(implicit caps: CAPSSession): CAPSRecords = {
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

  private def createInternal(header: RecordHeader, data: DataFrame)(implicit caps: CAPSSession) =
    new CAPSRecords(header, data) {}

  @tailrec
  private def containsEntity(t: CypherType): Boolean = t match {
    case _: CTNode         => true
    case _: CTRelationship => true
    case l: CTList         => containsEntity(l.elementType)
    case _                 => false
  }

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit caps: CAPSSession): CAPSRecords = {
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
