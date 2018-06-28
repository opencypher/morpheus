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
import org.opencypher.okapi.impl.table._
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.CAPSEntityTable
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.convert.rowToCypherMap
import org.opencypher.spark.impl.table.SparkFlatRelationalTable._

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
  table: DataFrameTable,
  override val logicalColumns: Option[Seq[String]] = None)
  extends RelationalCypherRecords[DataFrameTable] with RecordBehaviour {
  override type R = CAPSRecords

  def df: DataFrame = table.df

  override def from(header: RecordHeader, table: DataFrameTable, displayNames: Option[Seq[String]]): CAPSRecords = {
    copy(header, table, displayNames)
  }

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
