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

import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.RecordHeader
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.impl.flat.FreshVariableNamer
import org.opencypher.caps.impl.spark.physical.operators.PhysicalOperator.{assertIsNode, columnName, joinDFs, joinRecords}
import org.opencypher.caps.impl.spark.physical.{PhysicalResult, RuntimeContext}

sealed abstract class BinaryPhysicalOperator extends PhysicalOperator {

  def left: PhysicalOperator

  def right: PhysicalOperator

  override def children = Seq(left, right)

  override def execute(implicit context: RuntimeContext): PhysicalResult = executeBinary(left.execute, right.execute)

  def executeBinary(left: PhysicalResult, right: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult
}

final case class ValueJoin(left: PhysicalOperator,
                           right: PhysicalOperator,
                           predicates: Set[org.opencypher.caps.api.expr.Equals],
                           header: RecordHeader)
    extends BinaryPhysicalOperator {

  override def executeBinary(left: PhysicalResult, right: PhysicalResult)
                                      (implicit context: RuntimeContext): PhysicalResult = {
    val leftHeader = left.records.header
    val rightHeader = right.records.header
    val slots = predicates.map { p =>
      leftHeader.slotsFor(p.lhs).head -> rightHeader.slotsFor(p.rhs).head
    }.toSeq

    PhysicalResult(joinRecords(header, slots)(left.records, right.records), left.graphs ++ right.graphs)
  }

  override def internalCopy(newChildren: Seq[PhysicalOperator]): PhysicalOperator =
    copy(left = newChildren.head, right = newChildren(1))
}

final case class Optional(left: PhysicalOperator,
                          right: PhysicalOperator,
                          lhsHeader: RecordHeader,
                          rhsHeader: RecordHeader) extends BinaryPhysicalOperator {

  override def executeBinary(left: PhysicalResult, right: PhysicalResult)
                            (implicit context: RuntimeContext): PhysicalResult = {
    val lhsData = left.records.toDF()
    val rhsData = right.records.toDF()
    val commonFields = rhsHeader.fields.intersect(lhsHeader.fields)

    // Remove all common columns from the right hand side, except the join columns
    val columnsToRemove = commonFields
      .flatMap(rhsHeader.childSlots)
      .map(_.content)
      .map(columnName)
      .toSeq

    val lhsJoinSlots = commonFields.map(lhsHeader.slotFor)
    val rhsJoinSlots = commonFields.map(rhsHeader.slotFor)

    // Find the join pairs and introduce an alias for the right hand side
    // This is necessary to be able to deduplicate the join columns later
    val joinColumnMapping = lhsJoinSlots
      .map(lhsSlot => {
        lhsSlot -> rhsJoinSlots.find(_.content == lhsSlot.content).get
      })
      .map(pair => {
        val lhsCol = lhsData.col(columnName(pair._1))
        val rhsColName = columnName(pair._2)

        (lhsCol, rhsColName, FreshVariableNamer.generateUniqueName(rhsHeader))
      })
      .toSeq

    val reducedRhsData = joinColumnMapping
      .foldLeft(rhsData)((acc, col) => acc.withColumnRenamed(col._2, col._3))
      .drop(columnsToRemove: _*)

    val joinCols = joinColumnMapping.map(t => t._1 -> reducedRhsData.col(t._3))

    val joinedRecords =
      joinDFs(lhsData, reducedRhsData, rhsHeader, joinCols)("leftouter", deduplicate = true)(left.records.caps)

    PhysicalResult(joinedRecords, left.graphs ++ right.graphs)
  }

  override def internalCopy(newChildren: Seq[PhysicalOperator]): PhysicalOperator =
    copy(left = newChildren.head, right = newChildren(1))
}

// This maps a Cypher pattern such as (s)-[r]->(t), where s and t are both solved by lhs, and r is solved by rhs
final case class ExpandInto(left: PhysicalOperator,
                            right: PhysicalOperator,
                            source: Var,
                            rel: Var,
                            target: Var,
                            header: RecordHeader) extends BinaryPhysicalOperator {

  override def executeBinary(left: PhysicalResult, right: PhysicalResult)
                                      (implicit context: RuntimeContext): PhysicalResult = {
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

  override def internalCopy(newChildren: Seq[PhysicalOperator]): PhysicalOperator =
    copy(left = newChildren.head, right = newChildren(1))
}

final case class CartesianProduct(left: PhysicalOperator,
                                  right: PhysicalOperator,
                                  header: RecordHeader) extends BinaryPhysicalOperator {

  override def executeBinary(left: PhysicalResult, right: PhysicalResult)
                                      (implicit context: RuntimeContext): PhysicalResult = {
    val data = left.records.data
    val otherData = right.records.data
    val newData = data.crossJoin(otherData)
    val records = CAPSRecords.create(header, newData)(left.records.caps)
    val graphs = left.graphs ++ right.graphs
    PhysicalResult(records, graphs)
  }

  override def internalCopy(newChildren: Seq[PhysicalOperator]): PhysicalOperator =
    copy(left = newChildren.head, right = newChildren(1))
}
