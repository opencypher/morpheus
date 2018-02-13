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
package org.opencypher.caps.impl.spark.physical.operators

import org.apache.spark.sql.functions
import org.opencypher.caps.impl.table.{OpaqueField, RecordHeader, RecordSlot}
import org.opencypher.caps.impl.spark.physical.operators.CAPSPhysicalOperator.{assertIsNode, columnName, joinDFs, joinRecords}
import org.opencypher.caps.impl.spark.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.caps.impl.spark.{CAPSRecords, ColumnNameGenerator}
import org.opencypher.caps.ir.api.expr.Var
import org.opencypher.caps.impl.spark.DataFrameOps._

private[spark] abstract class BinaryPhysicalOperator extends CAPSPhysicalOperator {

  def lhs: CAPSPhysicalOperator

  def rhs: CAPSPhysicalOperator

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = executeBinary(lhs.execute, rhs.execute)

  def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult
}

final case class ValueJoin(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator,
    predicates: Set[org.opencypher.caps.ir.api.expr.Equals],
    header: RecordHeader)
    extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
      implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val leftHeader = left.records.header
    val rightHeader = right.records.header
    val slots = predicates.map { p =>
      leftHeader.slotsFor(p.lhs).head -> rightHeader.slotsFor(p.rhs).head
    }.toSeq

    CAPSPhysicalResult(joinRecords(header, slots)(left.records, right.records), left.graphs ++ right.graphs)
  }

}

/**
  * This operator performs a left outer join between the already matched path and the optional matched pattern and
  * updates the resulting columns.
  *
  * @param lhs previous match data
  * @param rhs optional match data
  * @param header result header (lhs header + rhs header)
  */
final case class Optional(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator, header: RecordHeader)
    extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
      implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val leftData = left.records.toDF()
    val rightData = right.records.toDF()
    val leftHeader = left.records.header
    val rightHeader = right.records.header

    val commonFields = leftHeader.slots.intersect(rightHeader.slots)

    val (joinSlots, otherCommonSlots) = commonFields.partition {
      case RecordSlot(_, _: OpaqueField) => true
      case RecordSlot(_, _)              => false
    }

    val joinFields = joinSlots
      .map(_.content)
      .collect { case OpaqueField(v) => v }

    val otherCommonFields = otherCommonSlots
      .map(_.content)

    val columnsToRemove = joinFields
      .flatMap(rightHeader.childSlots)
      .map(_.content)
      .union(otherCommonFields)
      .map(columnName)

    val lhsJoinSlots = joinFields.map(leftHeader.slotFor)
    val rhsJoinSlots = joinFields.map(rightHeader.slotFor)

    // Find the join pairs and introduce an alias for the right hand side
    // This is necessary to be able to deduplicate the join columns later
    val joinColumnMapping = lhsJoinSlots
      .map(lhsSlot => {
        lhsSlot -> rhsJoinSlots.find(_.content == lhsSlot.content).get
      })
      .map(pair => {
        val lhsColName = columnName(pair._1)
        val rhsColName = columnName(pair._2)

        (lhsColName, rhsColName, ColumnNameGenerator.generateUniqueName(rightHeader))
      })

    // Rename join columns on the right hand side and drop common non-join columns
    val reducedRhsData = joinColumnMapping
      .foldLeft(rightData)((acc, col) => acc.safeRenameColumn(col._2, col._3))
      .safeDropColumns(columnsToRemove: _*)

    val joinCols = joinColumnMapping.map(t => t._1 -> t._3)
    val joinedRecords =
      joinDFs(left.records.data, reducedRhsData, header, joinCols)("leftouter", deduplicate = true)(left.records.caps)

    CAPSPhysicalResult(joinedRecords, left.graphs ++ right.graphs)
  }
}

/**
  * This operator performs a left outer join between the already matched path and the pattern path. If, for a given,
  * already bound match, there is a non-null partner, we set a target column to true, otherwise false.
  * Only the mandatory match data and the target column are kept in the result.
  *
  * @param lhs mandatory match data
  * @param rhs expanded pattern predicate data
  * @param targetField field that will store the subquery value (exists true/false)
  * @param header result header (lhs header + predicateField)
  */
final case class ExistsSubQuery(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator,
    targetField: Var,
    header: RecordHeader)
    extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
      implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val leftData = left.records.toDF()
    val rightData = right.records.toDF()
    val leftHeader = left.records.header
    val rightHeader = right.records.header

    val joinFields = leftHeader.internalHeader.fields.intersect(rightHeader.internalHeader.fields)

    val columnsToRemove = joinFields
      .flatMap(rightHeader.childSlots)
      .map(_.content)
      .map(columnName)
      .toSeq

    val lhsJoinSlots = joinFields.map(leftHeader.slotFor)
    val rhsJoinSlots = joinFields.map(rightHeader.slotFor)

    // Find the join pairs and introduce an alias for the right hand side
    // This is necessary to be able to deduplicate the join columns later
    val joinColumnMapping = lhsJoinSlots
      .map(lhsSlot => {
        lhsSlot -> rhsJoinSlots.find(_.content == lhsSlot.content).get
      })
      .map(pair => {
        val lhsCol = columnName(pair._1)
        val rhsColName = columnName(pair._2)

        (lhsCol, rhsColName, ColumnNameGenerator.generateUniqueName(rightHeader))
      })
      .toSeq

    // Rename join columns on the right hand side and drop common non-join columns
    val reducedRhsData = joinColumnMapping
      .foldLeft(rightData)((acc, col) => acc.safeRenameColumn(col._2, col._3))
      .safeDropColumns(columnsToRemove: _*)

    // Compute distinct rows based on join columns
    val distinctRightData = reducedRhsData.dropDuplicates(joinColumnMapping.map(_._3))

    val joinCols = joinColumnMapping.map(t => t._1 -> t._3)

    val joinedRecords =
      joinDFs(left.records.data, distinctRightData, header, joinCols)("leftouter", deduplicate = true)(left.records.caps)

    val targetFieldColumnName = columnName(rightHeader.slotFor(targetField))
    val targetFieldColumn = joinedRecords.data.col(targetFieldColumnName)

    // If the targetField column contains no value we replace it with false, otherwise true.
    // After that we drop all right columns and only keep the predicate field.
    // The predicate field is later checked by a filter operator.
    val updatedJoinedRecords = joinedRecords.data
      .safeReplaceColumn(
        targetFieldColumnName,
        functions.when(functions.isnull(targetFieldColumn), false).otherwise(true))

    CAPSPhysicalResult(CAPSRecords.verifyAndCreate(header, updatedJoinedRecords)(left.records.caps), left.graphs ++ right.graphs)
  }
}

// This maps a Cypher pattern such as (s)-[r]->(t), where s and t are both solved by lhs, and r is solved by rhs
final case class ExpandInto(
    lhs: CAPSPhysicalOperator,
    rhs: CAPSPhysicalOperator,
    source: Var,
    rel: Var,
    target: Var,
    header: RecordHeader)
    extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
      implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val sourceSlot = left.records.header.slotFor(source)
    val targetSlot = left.records.header.slotFor(target)
    val relSourceSlot = right.records.header.sourceNodeSlot(rel)
    val relTargetSlot = right.records.header.targetNodeSlot(rel)

    assertIsNode(sourceSlot)
    assertIsNode(targetSlot)
    assertIsNode(relSourceSlot)
    assertIsNode(relTargetSlot)

    val joinedRecords =
      joinRecords(header, Seq(sourceSlot -> relSourceSlot, targetSlot -> relTargetSlot))(left.records, right.records)
    CAPSPhysicalResult(joinedRecords, left.graphs ++ right.graphs)
  }

}

/**
  * Computes the union of the two input operators. The two inputs must have identical headers.
  * This operation does not remove duplicates.
  *
  * The output header of this operation is identical to the input headers.
  *
  * @param lhs the first operand
  * @param rhs the second operand
  */
final case class Union(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator)
  extends BinaryPhysicalOperator with InheritedHeader {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext) = {
    val leftData = left.records.data
    // left and right have the same set of columns, but the order must also match
    val rightData = right.records.data.select(leftData.columns.head, leftData.columns.tail: _*)

    val unionedData = leftData.union(rightData)
    val records = CAPSRecords.verifyAndCreate(header, unionedData)(left.records.caps)

    CAPSPhysicalResult(records, left.graphs ++ right.graphs)
  }
}

final case class CartesianProduct(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator, header: RecordHeader)
    extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
      implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val data = left.records.data
    val otherData = right.records.data
    val newData = data.crossJoin(otherData)
    val records = CAPSRecords.verifyAndCreate(header, newData)(left.records.caps)
    val graphs = left.graphs ++ right.graphs
    CAPSPhysicalResult(records, graphs)
  }

}
