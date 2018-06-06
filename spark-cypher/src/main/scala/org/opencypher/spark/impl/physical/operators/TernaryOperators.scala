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
import org.opencypher.okapi.ir.api.expr.{EndNode, StartNode, Var}
import org.opencypher.okapi.logical.impl.{Directed, Direction, Undirected}
import org.opencypher.okapi.relational.impl.table.RecordHeaderNew
import org.opencypher.spark.impl.CAPSFunctions._
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator._
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}

private[spark] abstract class TernaryPhysicalOperator extends CAPSPhysicalOperator {

  def first: CAPSPhysicalOperator

  def second: CAPSPhysicalOperator

  def third: CAPSPhysicalOperator

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    executeTernary(first.execute, second.execute, third.execute)
  }

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
    header: RecordHeaderNew,
    isExpandInto: Boolean)
    extends TernaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeTernary(first: CAPSPhysicalResult, second: CAPSPhysicalResult, third: CAPSPhysicalResult)
    (implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val expanded = expand(first.records, second.records)

    CAPSPhysicalResult(finalize(expanded, third.records), first.workingGraph, first.workingGraphName)
  }

  private def iterate(lhs: DataFrame, lhsHeader: RecordHeaderNew, rels: DataFrame, relsHeader: RecordHeaderNew)(
      endNode: Var,
      rel: Var,
      relStartNode: StartNode,
      listTempColName: String,
      edgeListColName: String,
      keep: Array[String]): DataFrame = {

    val relIdColumn = rels.col(relsHeader.column(rel))
    val startColumn = rels.col(relsHeader.column(relStartNode))
    val expandColumnName = lhsHeader.column(endNode)
    val expandColumn = lhs.col(expandColumnName)

    val joined = lhs.join(rels, expandColumn === startColumn, "inner")

    val extendedArray = array_append_long(lhs.col(edgeListColName), relIdColumn)
    val withExtendedArray = joined.safeAddColumn(listTempColName, extendedArray)
    val arrayContains = array_contains(withExtendedArray.col(edgeListColName), relIdColumn)
    val filtered = withExtendedArray.filter(!arrayContains)

    // TODO: Try and get rid of the Var rel here
    val endNodeIdColNameOfJoinedRel = relsHeader.column(EndNode(rel)(CTNode))

    val columns = keep ++ Seq(listTempColName, endNodeIdColNameOfJoinedRel)
    val withoutRelProperties = filtered.select(columns.head, columns.tail: _*) // drops joined columns from relationship table

    withoutRelProperties
      .safeDropColumn(expandColumnName)
      .safeRenameColumn(endNodeIdColNameOfJoinedRel, expandColumnName)
      .safeDropColumn(edgeListColName)
      .safeRenameColumn(listTempColName, edgeListColName)
  }

  private def finalize(expanded: CAPSRecords, targets: CAPSRecords): CAPSRecords = {
    val endNodeColumn = expanded.header.column(initialEndNode)
    val targetNodeColumn = targets.header.column(target)

    // If the expansion ends in an already solved plan, the final join can be replaced by a filter.
    val result = if (isExpandInto) {
      val data = expanded.toDF()
      CAPSRecords(header, data.filter(data.col(targetNodeColumn) === data.col(endNodeColumn)))(expanded.caps)
    } else {
      val joinHeader = expanded.header ++ targets.header

      assertIsNode(initialEndNode)
      assertIsNode(target)

      val lhsColumn = expanded.header.column(initialEndNode)
      val rhsColumn = targets.header.column(target)

      joinRecords(joinHeader, Seq(lhsColumn -> rhsColumn))(expanded, targets)
    }

    CAPSRecords(header, result.toDF().safeDropColumn(endNodeColumn))(expanded.caps)
  }

  private def expand(firstRecords: CAPSRecords, secondRecords: CAPSRecords): CAPSRecords = {
    val initData = firstRecords.df
    val relsData = direction match {
      case Directed =>
        secondRecords.df
      case Undirected =>
        // TODO this is a crude hack that will not work once we have proper path support
        val startNodeSlot = secondRecords.header.column(secondRecords.header.startNodeFor(rel))
        val endNodeSlot = secondRecords.header.column(secondRecords.header.endNodeFor(rel))
        val columns = secondRecords.header.columns.toSeq.sorted

        val inverted = secondRecords.df
          .safeRenameColumn(startNodeSlot, "__tmp__")
          .safeRenameColumn(endNodeSlot, startNodeSlot)
          .safeRenameColumn("__tmp__", endNodeSlot)
          .select(columns.head, columns.tail: _*)

        inverted.union(secondRecords.df)
    }

    def generateUniqueName = s"tmp${System.nanoTime}"

    val edgeListColName = firstRecords.header.column(edgeList)

    val steps = new collection.mutable.HashMap[Int, DataFrame]
    steps(0) = initData

    val keep = initData.columns

    val listTempColName = generateUniqueName

    val relStartNode = secondRecords.header.startNodeFor(rel)
    (1 to upper).foreach { i =>
      // TODO: Check whether we can abort iteration if result has no cardinality (eg count > 0?)
      steps(i) = iterate(steps(i - 1), firstRecords.header, relsData, secondRecords.header)(initialEndNode, rel, relStartNode, listTempColName, edgeListColName, keep)
    }

    val union = steps.filterKeys(_ >= lower).values.reduce[DataFrame] {
      case (l, r) => l.union(r)
    }

    CAPSRecords(firstRecords.header, union)(firstRecords.caps)
  }
}
