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
package org.opencypher.spark.impl.physical.operators

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.ir.api.expr.{EndNode, Var}
import org.opencypher.okapi.logical.impl.{Directed, Direction, Undirected}
import org.opencypher.okapi.relational.impl.ColumnNameGenerator
import org.opencypher.okapi.relational.impl.table.{OpaqueField, ProjectedExpr, RecordHeader, RecordSlot}
import org.opencypher.spark.impl.CAPSFunctions._
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator._
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.spark.impl.CAPSRecords

private[spark] abstract class TernaryPhysicalOperator extends CAPSPhysicalOperator {

  def first: CAPSPhysicalOperator

  def second: CAPSPhysicalOperator

  def third: CAPSPhysicalOperator

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult =
    executeTernary(first.execute, second.execute, third.execute)

  def executeTernary(first: CAPSPhysicalResult, second: CAPSPhysicalResult, third: CAPSPhysicalResult)(
      implicit context: CAPSRuntimeContext): CAPSPhysicalResult
}

// Expands a pattern like (s)-[r*n..m]->(t) where s is solved by first, r is solved by second and t is solved by third
// this performs m joins with second to step all steps, then drops n of these steps
// edgeList is what is bound to r; a list of relationships (currently just the ids)
final case class BoundedVarExpand(
    first: CAPSPhysicalOperator,
    second: CAPSPhysicalOperator,
    third: CAPSPhysicalOperator,
    rel: Var,
    edgeList: Var,
    target: Var,
    initialEndNode: Var,
    lower: Int,
    upper: Int,
    direction: Direction,
    header: RecordHeader,
    isExpandInto: Boolean)
    extends TernaryPhysicalOperator {

  override def executeTernary(first: CAPSPhysicalResult, second: CAPSPhysicalResult, third: CAPSPhysicalResult)
    (implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val expanded = expand(first.records, second.records)

    CAPSPhysicalResult(finalize(expanded, third.records), first.workingGraph, first.workingGraphName)
  }

  private def iterate(lhs: DataFrame, rels: DataFrame)(
      endNode: RecordSlot,
      rel: Var,
      relStartNode: RecordSlot,
      listTempColName: String,
      edgeListColName: String,
      keep: Array[String]): DataFrame = {

    val relIdColumn = rels.col(columnName(OpaqueField(rel)))
    val startColumn = rels.col(columnName(relStartNode))
    val expandColumnName = columnName(endNode)
    val expandColumn = lhs.col(expandColumnName)

    val joined = lhs.join(rels, expandColumn === startColumn, "inner")

    val extendedArray = array_append_long(lhs.col(edgeListColName), relIdColumn)
    val withExtendedArray = joined.safeAddColumn(listTempColName, extendedArray)
    val arrayContains = array_contains(withExtendedArray.col(edgeListColName), relIdColumn)
    val filtered = withExtendedArray.filter(!arrayContains)

    // TODO: Try and get rid of the Var rel here
    val endNodeIdColNameOfJoinedRel = columnName(ProjectedExpr(EndNode(rel)(CTNode)))

    val columns = keep ++ Seq(listTempColName, endNodeIdColNameOfJoinedRel)
    val withoutRelProperties = filtered.select(columns.head, columns.tail: _*) // drops joined columns from relationship table

    withoutRelProperties
      .safeDropColumn(expandColumnName)
      .safeRenameColumn(endNodeIdColNameOfJoinedRel, expandColumnName)
      .safeDropColumn(edgeListColName)
      .safeRenameColumn(listTempColName, edgeListColName)
  }

  private def finalize(expanded: CAPSRecords, targets: CAPSRecords): CAPSRecords = {
    val endNodeSlot = expanded.header.slotFor(initialEndNode)
    val endNodeCol = columnName(endNodeSlot)

    val targetNodeSlot = targets.header.slotFor(target)
    val targetNodeCol = columnName(targetNodeSlot)

    // If the expansion ends in an already solved plan, the final join can be replaced by a filter.
    val result = if (isExpandInto) {
      val data = expanded.toDF()
      CAPSRecords.verifyAndCreate(header, data.filter(data.col(targetNodeCol) === data.col(endNodeCol)))(expanded.caps)
    } else {
      val joinHeader = expanded.header ++ targets.header

      val lhsSlot = expanded.header.slotFor(initialEndNode)
      val rhsSlot = targets.header.slotFor(target)

      assertIsNode(lhsSlot)
      assertIsNode(rhsSlot)

      joinRecords(joinHeader, Seq(lhsSlot -> rhsSlot))(expanded, targets)
    }

    CAPSRecords.verifyAndCreate(header, result.toDF().safeDropColumn(endNodeCol))(expanded.caps)
  }

  private def expand(firstRecords: CAPSRecords, secondRecords: CAPSRecords): CAPSRecords = {
    val initData = firstRecords.data
    val relsData = direction match {
      case Directed =>
        secondRecords.data
      case Undirected =>
        // TODO this is a crude hack that will not work once we have proper path support
        val startNodeSlot = columnName(secondRecords.header.sourceNodeSlot(rel))
        val endNodeSlot = columnName(secondRecords.header.targetNodeSlot(rel))
        val colOrder = secondRecords.header.slots.map(columnName)

        val inverted = secondRecords.data
          .safeRenameColumn(startNodeSlot, "__tmp__")
          .safeRenameColumn(endNodeSlot, startNodeSlot)
          .safeRenameColumn("__tmp__", endNodeSlot)
          .select(colOrder.head, colOrder.tail: _*)

        inverted.union(secondRecords.data)
    }

    val edgeListColName = columnName(firstRecords.header.slotFor(edgeList))

    val steps = new collection.mutable.HashMap[Int, DataFrame]
    steps(0) = initData

    val keep = initData.columns

    val listTempColName =
      ColumnNameGenerator.generateUniqueName(firstRecords.header)

    val startSlot = secondRecords.header.sourceNodeSlot(rel)
    val endNodeSlot = firstRecords.header.slotFor(initialEndNode)
    (1 to upper).foreach { i =>
      // TODO: Check whether we can abort iteration if result has no cardinality (eg count > 0?)
      steps(i) = iterate(steps(i - 1), relsData)(endNodeSlot, rel, startSlot, listTempColName, edgeListColName, keep)
    }

    val union = steps.filterKeys(_ >= lower).values.reduce[DataFrame] {
      case (l, r) => l.union(r)
    }

    CAPSRecords.verifyAndCreate(firstRecords.header, union)(firstRecords.caps)
  }
}
