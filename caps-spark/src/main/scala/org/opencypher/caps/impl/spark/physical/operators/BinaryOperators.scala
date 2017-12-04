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

import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.caps.api.expr.{Expr, Var}
import org.opencypher.caps.api.record.{OpaqueField, RecordHeader, RecordSlot, SlotContent}
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.impl.spark.{ColumnNameGenerator, SparkColumnName}
import org.opencypher.caps.impl.spark.physical.operators.PhysicalOperator.{assertIsNode, columnName, joinDFs, joinRecords}
import org.opencypher.caps.impl.spark.physical.{PhysicalResult, RuntimeContext}

private[spark] abstract class BinaryPhysicalOperator extends PhysicalOperator {

  def lhs: PhysicalOperator

  def rhs: PhysicalOperator

  override def execute(implicit context: RuntimeContext): PhysicalResult = executeBinary(lhs.execute, rhs.execute)

  def executeBinary(left: PhysicalResult, right: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult
}

final case class ValueJoin(
    lhs: PhysicalOperator,
    rhs: PhysicalOperator,
    predicates: Set[org.opencypher.caps.api.expr.Equals],
    header: RecordHeader)
    extends BinaryPhysicalOperator {

  override def executeBinary(left: PhysicalResult, right: PhysicalResult)(
      implicit context: RuntimeContext): PhysicalResult = {
    val leftHeader = left.records.header
    val rightHeader = right.records.header
    val slots = predicates.map { p =>
      leftHeader.slotsFor(p.lhs).head -> rightHeader.slotsFor(p.rhs).head
    }.toSeq

    PhysicalResult(joinRecords(header, slots)(left.records, right.records), left.graphs ++ right.graphs)
  }

}
abstract class PatternJoin extends BinaryPhysicalOperator {

  override def executeBinary(left: PhysicalResult, right: PhysicalResult)
                            (implicit context: RuntimeContext): PhysicalResult = {
    val leftData = left.records.toDF()
    val rightData = right.records.toDF()
    val leftHeader = left.records.header
    val rightHeader = right.records.header

    val commonFields = leftHeader.slots.intersect(rightHeader.slots)

    val (joinSlots, otherCommonSlots) = commonFields.partition {
      case RecordSlot(_, _: OpaqueField) => true
      case RecordSlot(_, _) => false
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
          val lhsCol = leftData.col(columnName(pair._1))
          val rhsColName = columnName(pair._2)

          (lhsCol, rhsColName, ColumnNameGenerator.generateUniqueName(rightHeader))
        })
        .toSeq

    // Rename join columns on the right hand side and drop common non-join columns
    val reducedRhsData = joinColumnMapping
        .foldLeft(rightData)((acc, col) => acc.withColumnRenamed(col._2, col._3))
        .drop(columnsToRemove: _*)

    executeInternal(left, right, rightHeader, joinColumnMapping, reducedRhsData)
  }

  def executeInternal(
      left: PhysicalResult,
      right: PhysicalResult,
      rhsHeader: RecordHeader,
      joinColumnMapping: Seq[(Column, String, String)],
      reducedRhsData: DataFrame): PhysicalResult
}

final case class Optional(lhs: PhysicalOperator, rhs: PhysicalOperator, header: RecordHeader)
    extends PatternJoin {

  override def executeInternal(
      left: PhysicalResult,
      right: PhysicalResult,
      rhsHeader: RecordHeader,
      joinColumnMapping: Seq[(Column, String, String)],
      reducedRhsData: DataFrame): PhysicalResult = {

    val joinCols = joinColumnMapping.map(t => t._1 -> reducedRhsData.col(t._3))

    val joinedRecords =
      joinDFs(left.records.data, reducedRhsData, header, joinCols)("leftouter", deduplicate = true)(left.records.caps)

    PhysicalResult(joinedRecords, left.graphs ++ right.graphs)
  }
}

/**
  * This operator performs a left outer join between the mandatory path and the pattern path. If, for a given mandatory
  * match, there is a non-null partner, we set a predicate column to true, otherwise false. Only the mandatory match
  * data and the predicate column are kept in the result.
  *
  * @param lhs mandatory match data
  * @param rhs expanded pattern predicate data
  * @param predicateField field that will store the predicate value
  * @param header result header (lhs header + predicateField)
  */
final case class PatternPredicate(
    lhs: PhysicalOperator,
    rhs: PhysicalOperator,
    predicateField: Var,
    header: RecordHeader)
    extends PatternJoin {

  override def executeInternal(
      left: PhysicalResult,
      right: PhysicalResult,
      rhsHeader: RecordHeader,
      joinColumnMapping: Seq[(Column, String, String)],
      reducedRhsData: DataFrame): PhysicalResult = {

    // Compute distinct rows based on join columns
    val distinctRightData = reducedRhsData.dropDuplicates(joinColumnMapping.map(_._3))

    val joinCols = joinColumnMapping.map(t => t._1 -> distinctRightData.col(t._3))

    val joinedRecords =
      joinDFs(left.records.data, distinctRightData, rhsHeader, joinCols)("leftouter", deduplicate = true)(left.records.caps)

    val rightNonJoinColumns = distinctRightData.columns.toSet -- joinColumnMapping.map(_._3)

    // We only need to check one non-join column for its value to decide if the pattern exists
    assert(rightNonJoinColumns.nonEmpty)
    val rightNonJoinColumn = distinctRightData.col(rightNonJoinColumns.head)

    // If the non-join column contains no value we set the predicate field to false, otherwise true.
    // After that we drop all right columns and only keep the predicate field.
    // The predicate field is later checked by a filter operator.
    val updatedJoinedRecords = joinedRecords.data
      .withColumn(
        SparkColumnName.of(header.slotFor(predicateField)),
        functions.when(functions.isnull(rightNonJoinColumn), false).otherwise(true))
      .drop(rightNonJoinColumns.toSeq ++ joinColumnMapping.map(_._3): _*)

    PhysicalResult(CAPSRecords.create(header, updatedJoinedRecords)(left.records.caps), left.graphs ++ right.graphs)
  }
}

// This maps a Cypher pattern such as (s)-[r]->(t), where s and t are both solved by lhs, and r is solved by rhs
final case class ExpandInto(
    lhs: PhysicalOperator,
    rhs: PhysicalOperator,
    source: Var,
    rel: Var,
    target: Var,
    header: RecordHeader)
    extends BinaryPhysicalOperator {

  override def executeBinary(left: PhysicalResult, right: PhysicalResult)(
      implicit context: RuntimeContext): PhysicalResult = {
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
    PhysicalResult(joinedRecords, left.graphs ++ right.graphs)
  }

}

final case class CartesianProduct(lhs: PhysicalOperator, rhs: PhysicalOperator, header: RecordHeader)
    extends BinaryPhysicalOperator {

  override def executeBinary(left: PhysicalResult, right: PhysicalResult)(
      implicit context: RuntimeContext): PhysicalResult = {
    val data = left.records.data
    val otherData = right.records.data
    val newData = data.crossJoin(otherData)
    val records = CAPSRecords.create(header, newData)(left.records.caps)
    val graphs = left.graphs ++ right.graphs
    PhysicalResult(records, graphs)
  }

}
