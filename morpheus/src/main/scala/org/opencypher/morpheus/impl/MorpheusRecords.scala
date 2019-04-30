/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.morpheus.impl

import java.util.Collections

import org.apache.spark.sql._
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.impl.convert.SparkConversions._
import org.opencypher.morpheus.impl.convert.rowToCypherMap
import org.opencypher.morpheus.impl.table.SparkTable.{DataFrameTable, _}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.relational.api.io.ElementTable
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, RelationalCypherRecordsFactory}
import org.opencypher.okapi.relational.impl.table._

import scala.collection.JavaConverters._

case class MorpheusRecordsFactory()(implicit morpheus: MorpheusSession) extends RelationalCypherRecordsFactory[DataFrameTable] {

  override type Records = MorpheusRecords

  override def unit(): MorpheusRecords = {
    val initialDataFrame = morpheus.sparkSession.createDataFrame(Seq(EmptyRow()))
    MorpheusRecords(RecordHeader.empty, initialDataFrame)
  }

  override def empty(initialHeader: RecordHeader = RecordHeader.empty): MorpheusRecords = {
    val initialSparkStructType = initialHeader.toStructType
    val initialDataFrame = morpheus.sparkSession.createDataFrame(Collections.emptyList[Row](), initialSparkStructType)
    MorpheusRecords(initialHeader, initialDataFrame)
  }

  override def fromElementTable(elementTable: ElementTable[DataFrameTable]): MorpheusRecords = {
    val withCypherCompatibleTypes = elementTable.table.df.withCypherCompatibleTypes
    MorpheusRecords(elementTable.header, withCypherCompatibleTypes)
  }

  override def from(
    header: RecordHeader,
    table: DataFrameTable,
    maybeDisplayNames: Option[Seq[String]]
  ): MorpheusRecords = {
    val displayNames = maybeDisplayNames match {
      case s@Some(_) => s
      case None => Some(header.vars.map(_.withoutType).toSeq)
    }
    MorpheusRecords(header, table, displayNames)
  }

  /**
    * Wraps a Spark SQL table (DataFrame) in a MorpheusRecords, making it understandable by Cypher.
    *
    * @param df   table to wrap.
    * @param morpheus session to which the resulting MorpheusRecords is tied.
    * @return a Cypher table.
    */
  private[morpheus] def wrap(df: DataFrame)(implicit morpheus: MorpheusSession): MorpheusRecords = {
    val compatibleDf = df.withCypherCompatibleTypes
    MorpheusRecords(compatibleDf.schema.toRecordHeader, compatibleDf)
  }

  private case class EmptyRow()
}

case class MorpheusRecords(
  header: RecordHeader,
  table: DataFrameTable,
  override val logicalColumns: Option[Seq[String]] = None
)(implicit morpheus: MorpheusSession) extends RelationalCypherRecords[DataFrameTable] with RecordBehaviour {
  override type Records = MorpheusRecords

  def df: DataFrame = table.df

  override def cache(): MorpheusRecords = {
    df.cache()
    this
  }

  override def toString: String = {
    if (header.isEmpty) {
      s"MorpheusRecords.empty"
    } else {
      s"MorpheusRecords(header: $header)"
    }
  }
}

trait RecordBehaviour extends RelationalCypherRecords[DataFrameTable] {

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


  def toCypherMaps: Dataset[CypherMap] = {
    import encoders._
    table.df.map(rowToCypherMap(header.exprToColumn.toSeq))
  }
}
