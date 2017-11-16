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
package org.opencypher.caps.impl.spark.physical.operators

import java.net.URI

import org.apache.spark.sql.{Column, DataFrame}
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSSession}
import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.common.AbstractTreeNode
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.spark.physical.{PhysicalResult, RuntimeContext}

private[spark] abstract class PhysicalOperator extends AbstractTreeNode[PhysicalOperator] {
  def execute(implicit context: RuntimeContext): PhysicalResult

  protected def resolve(uri: URI)(implicit context: RuntimeContext): CAPSGraph = {
    context.resolve(uri).getOrElse(Raise.graphNotFound(uri))
  }

  override def toString() = s"${super.toString()}($argString)"

  override def argFilter: Any => Boolean = {
    case _: RecordHeader => false
    case other => super.argFilter(other)
  }
}

object PhysicalOperator {
  def columnName(slot: RecordSlot): String = SparkColumnName.of(slot)

  def columnName(content: SlotContent): String = SparkColumnName.of(content)

  def joinRecords(
    header: RecordHeader,
    joinSlots: Seq[(RecordSlot, RecordSlot)],
    joinType: String = "inner",
    deduplicate: Boolean = false)(lhs: CAPSRecords, rhs: CAPSRecords): CAPSRecords = {

    val lhsData = lhs.toDF()
    val rhsData = rhs.toDF()

    val joinCols = joinSlots.map(pair => lhsData.col(columnName(pair._1)) -> rhsData.col(columnName(pair._2)))

    joinDFs(lhsData, rhsData, header, joinCols)(joinType, deduplicate)(lhs.caps)
  }

  def joinDFs(lhsData: DataFrame, rhsData: DataFrame, header: RecordHeader, joinCols: Seq[(Column, Column)])(
    joinType: String,
    deduplicate: Boolean)(implicit caps: CAPSSession): CAPSRecords = {

    val joinExpr = joinCols.map { case (l, r) => l === r }
      .reduce(_ && _)

    val joinedData = lhsData.join(rhsData, joinExpr, joinType)

    val returnData = if (deduplicate) {
      val colsToDrop = joinCols.map(col => col._2)
      colsToDrop.foldLeft(joinedData)((acc, col) => acc.drop(col))
    } else joinedData

    CAPSRecords.create(header, returnData)
  }

  def assertIsNode(slot: RecordSlot): Unit = {
    slot.content.cypherType match {
      case CTNode(_) =>
      case x =>
        throw new IllegalArgumentException(s"Expected $slot to contain a node, but was $x")
    }
  }

}
