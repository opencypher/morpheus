/*
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

import java.util.Collections

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
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
import org.opencypher.caps.impl.util.ColumnMappableDf

import scala.annotation.tailrec
import scala.reflect.runtime.universe.TypeTag

sealed abstract class CAPSRecords(
  override val header: RecordHeader,
  override val data: DataFrame
)(implicit val caps: CAPSSession)
  extends CypherRecords with Serializable {

  self =>

  override type Data = DataFrame
  override type Records = CAPSRecords

  override val fields: Set[String] = header.fields.map(_.name)
  override val fieldsInOrder: Seq[String] = header.fieldsInOrder.map(_.name)

  def sparkColumns: IndexedSeq[String] = header.internalHeader.columns

  //noinspection AccessorLikeMethodIsEmptyParen
  def toDF(): Data = data

  def mapDF(f: Data => Data): CAPSRecords = CAPSRecords.create(f(data))

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

  //  def repartition(numPartitions: Int): CAPSRecords =
  //    CAPSRecords.create(header, data.repartition(numPartitions))

  override def print(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(this)

  def select(fields: Set[Var]): CAPSRecords = {
    val selectedHeader = header.select(fields)
    val selectedColumnNames = selectedHeader.contents.map(SparkColumnName.of).toSeq
    val selectedColumns = data.select(selectedColumnNames.head, selectedColumnNames.tail: _*)
    CAPSRecords.create(selectedHeader, selectedColumns)
  }

  def compact(implicit details: RetainedDetails): CAPSRecords = {
    val cachedHeader = self.header.update(compactFields)._1
    if (header == cachedHeader) {
      this
    } else {
      val cachedData = {
        val columns = cachedHeader.slots.map(c => new Column(SparkColumnName.of(c.content)))
        self.data.select(columns: _*)
      }

      CAPSRecords.create(cachedHeader, cachedData)
    }
  }

  def unionAll(header: RecordHeader, other: CAPSRecords): CAPSRecords = {
    val unionData = data.union(other.data)
    CAPSRecords.create(header, unionData)
  }

  override def contract[E <: EmbeddedEntity](entity: VerifiedEmbeddedEntity[E]): CAPSRecords = {
    val slotExprs = entity.slots
    val newSlots = header.slots.map {
      case slot@RecordSlot(idx, content: FieldSlotContent) =>
        slotExprs.get(content.field.name).map {
          case expr: Var => OpaqueField(expr)
          case expr: Property => ProjectedExpr(expr.copy()(cypherType = content.cypherType))
          case expr => ProjectedExpr(expr)
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

  def toLocalScalaIterator: Iterator[CypherMap] = {
    import scala.collection.JavaConverters._

    toLocalIterator.asScala
  }

  def toLocalIterator: java.util.Iterator[CypherMap] = {
    toCypherMaps.toLocalIterator()
  }

  def foreachPartition(f: Iterator[CypherMap] => Unit): Unit = {
    toCypherMaps.foreachPartition(f)
  }

  def collect(): Array[CypherMap] =
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
    val toCast = initialDataFrame.schema.fields.filter(f => fromSparkType(f.dataType, f.nullable).isEmpty)
    val dfWithCompatibleTypes: DataFrame = toCast.foldLeft(initialDataFrame) { case (df, field) =>
        val castType = field.dataType match {
          case ByteType | ShortType | IntegerType => LongType
          case FloatType => DoubleType
          case other => Raise.unsupportedArgument(
            s"Cannot convert or cast type $other of field $field to a Spark type supported by Cypher")
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
          s"with columns ${initialHeader.internalHeader.columns.sorted.mkString("\n", ", ", "\n")}",
          s"with columns ${initialData.columns.sorted.mkString("\n", ", ", "\n")}"
        )
      }

      // Verify column types
      initialHeader.slots.foreach { slot =>
        val dfSchema = initialData.schema
        val field = dfSchema(SparkColumnName.of(slot))
        val cypherType = fromSparkType(field.dataType, field.nullable).getOrElse(
          Raise.invalidArgument("A supported Spark type", field.dataType.toString))
        val headerType = slot.content.cypherType

        // if the type in the data doesn't correspond to the type in the header we fail
        // except: we encode nodes, rels and integers with the same data type, so we can't fail
        // on conflicts when we expect entities (alternative: change reverse-mapping function somehow)
        if (toSparkType(headerType) != toSparkType(cypherType) && !containsEntity(headerType))
          Raise.invalidDataTypeForColumn(field.name, headerType.toString, cypherType.toString)
      }
      createInternal(initialHeader, initialData)
    }
    else {
      Raise.capsSessionMismatch()
    }
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
