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
package org.opencypher.spark.impl.physical.operators

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.relational.api.physical.PhysicalOperator
import org.opencypher.okapi.relational.impl.table.{ColumnName, RecordHeader, RecordSlot, SlotContent}
import org.opencypher.okapi.trees.AbstractTreeNode
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords}

private[spark] abstract class CAPSPhysicalOperator
  extends AbstractTreeNode[CAPSPhysicalOperator]
  with PhysicalOperator[CAPSRecords, CAPSGraph, CAPSRuntimeContext] {

  override def header: RecordHeader

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult

  protected def resolve(qualifiedGraphName: QualifiedGraphName)(implicit context: CAPSRuntimeContext): CAPSGraph = {
    context.resolve(qualifiedGraphName).map(_.asCaps).getOrElse(throw IllegalArgumentException(s"a graph at $qualifiedGraphName"))
  }

  override def args: Iterator[Any] = super.args.flatMap {
    case RecordHeader(_) | Some(RecordHeader(_)) => None
    case other                                   => Some(other)
  }
}

object CAPSPhysicalOperator {
  def columnName(slot: RecordSlot): String = ColumnName.of(slot)

  def columnName(content: SlotContent): String = ColumnName.of(content)

  def joinRecords(
      header: RecordHeader,
      joinSlots: Seq[(RecordSlot, RecordSlot)],
      joinType: String = "inner",
      deduplicate: Boolean = false)(lhs: CAPSRecords, rhs: CAPSRecords): CAPSRecords = {

    val lhsData = lhs.toDF()
    val rhsData = rhs.toDF()

    val joinCols = joinSlots.map(pair => columnName(pair._1) -> columnName(pair._2))

    joinDFs(lhsData, rhsData, header, joinCols)(joinType, deduplicate)(lhs.caps)
  }

  def joinDFs(lhsData: DataFrame, rhsData: DataFrame, header: RecordHeader, joinCols: Seq[(String, String)])(
      joinType: String,
      deduplicate: Boolean)(implicit caps: CAPSSession): CAPSRecords = {

    val joinedData = lhsData.safeJoin(rhsData, joinCols, joinType)

    val returnData = if (deduplicate) {
      val colsToDrop = joinCols.map(col => col._2)
      joinedData.safeDropColumns(colsToDrop: _*)
    } else joinedData

    CAPSRecords.verifyAndCreate(header, returnData)
  }

  def assertIsNode(slot: RecordSlot): Unit = {
    slot.content.cypherType match {
      case CTNode(_) =>
      case x =>
        throw IllegalArgumentException(s"Expected $slot to contain a node, but was $x")
    }
  }

}
