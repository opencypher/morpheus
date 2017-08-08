/**
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
package org.opencypher.spark.impl.physical

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, LongType}
import org.apache.spark.sql.{DataFrame, Row}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.api.ir.pattern.{AnyGiven, EveryNode}
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.spark.SparkCypherRecords
import org.opencypher.spark.api.types.{CTBoolean, CTNode}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.flat.FreshVariableNamer
import org.opencypher.spark.impl.instances.spark.SparkSQLExprMapper.asSparkSQLExpr
import org.opencypher.spark.impl.logical.NamedLogicalGraph
import org.opencypher.spark.impl.spark.SparkColumnName
import org.opencypher.spark.impl.syntax.expr._

import scala.collection.mutable

object RuntimeContext {
  val empty = RuntimeContext(Map.empty, TokenRegistry.empty, ConstantRegistry.empty)
}

case class RuntimeContext(parameters: Map[ConstantRef, CypherValue], tokens: TokenRegistry, constants: ConstantRegistry) {
  def columnName(slot: RecordSlot): String = SparkColumnName.of(slot)
  def columnName(content: SlotContent): String = SparkColumnName.of(content)
}

class PhysicalResultProducer(context: RuntimeContext) {

  implicit val c = context

  implicit final class RichCypherResult(val prev: PhysicalResult) {

    def nodeScan(inGraph: NamedLogicalGraph, v: Var, labels: EveryNode, header: RecordHeader): PhysicalResult = {
      val graph = prev.graphs(inGraph.name)

      val records = graph.nodes(v.name)

      // TODO: Should not discard prev records here
      prev.mapRecordsWithDetails(_ => records)
    }

    def relationshipScan(inGraph: NamedLogicalGraph, v: Var, header: RecordHeader): PhysicalResult = {
      val graph = prev.graphs(inGraph.name)

      val records = graph.relationships(v.name)

      // TODO: Should not discard prev records here
      prev.mapRecordsWithDetails(_ => records)
    }

    def filter(expr: Expr, header: RecordHeader): PhysicalResult = {
      prev.mapRecordsWithDetails { subject =>
        val filteredRows = asSparkSQLExpr(subject.header, expr, subject.data) match {
          case Some(sqlExpr) =>
            subject.data.where(sqlExpr)
          case None =>
            val predicate = cypherFilter(header, expr)
            subject.data.filter(liftTernary(predicate))
        }

        val selectedColumns = header.slots.map { c =>
          val name = context.columnName(c)
          filteredRows.col(name)
        }

        val newData = filteredRows.select(selectedColumns: _*)

        SparkCypherRecords.create(header, newData)(subject.space)
      }
    }

    def alias(expr: Expr, v: Var, header: RecordHeader): PhysicalResult =
      prev.mapRecordsWithDetails { subject =>
        val oldSlot = subject.header.slotsFor(expr).head

        val newSlot = header.slotsFor(v).head

        val oldColumnName = context.columnName(oldSlot)
        val newColumnName = context.columnName(newSlot)

        val newData = if (subject.data.columns.contains(oldColumnName)) {
          subject.data.withColumnRenamed(oldColumnName, newColumnName)
        } else {
          Raise.columnNotFound(oldColumnName)
        }

        SparkCypherRecords.create(header, newData)(subject.space)
      }

    def project(expr: Expr, header: RecordHeader): PhysicalResult =
      prev.mapRecordsWithDetails { subject =>
        val newData = asSparkSQLExpr(header, expr, subject.data) match {
          case None => Raise.notYetImplemented(s"projecting $expr")

          case Some(sparkSqlExpr) =>
            val headerNames = header.slotsFor(expr).map(context.columnName)
            val dataNames = subject.data.columns.toSeq

            // TODO: Can optimise for var AS var2 case -- avoid duplicating data
            headerNames.diff(dataNames) match {
              case Seq(one) =>
                // align the name of the column to what the header expects
                val newCol = sparkSqlExpr.as(one)
                val columnsToSelect = subject.data.columns.map(subject.data.col) :+ newCol

                subject.data.select(columnsToSelect: _*)
              case _ => Raise.multipleSlotsForExpression()
            }
        }

        SparkCypherRecords.create(header, newData)(subject.space)
      }

    def select(fields: IndexedSeq[Var], header: RecordHeader): PhysicalResult =
      prev.mapRecordsWithDetails { subject =>
        val fieldIndices = fields.zipWithIndex.toMap

        val groupedSlots = header.slots.sortBy {
          _.content match {
            case content: FieldSlotContent =>
              fieldIndices.getOrElse(content.field, Int.MaxValue)
            case content@ProjectedExpr(expr) =>
              val deps = expr.dependencies
              deps.headOption.filter(_ => deps.size == 1).flatMap(fieldIndices.get).getOrElse(Int.MaxValue)
          }
        }

        val data = subject.data
        val columns = groupedSlots.map { s => data.col(context.columnName(s)) }
        val newData = subject.data.select(columns: _*)

        SparkCypherRecords.create(header, newData)(subject.space)
      }

    def typeFilter(rel: Var, types: AnyGiven[RelTypeRef], header: RecordHeader): PhysicalResult = {
      if (types.elts.isEmpty) prev
      else {
        val typeExprs: Set[Expr] = types.elts.map { ref => HasType(rel, context.tokens.relType(ref))(CTBoolean) }
        prev.filter(Ors(typeExprs), header)
      }
    }

    def joinSource(relView: PhysicalResult, header: RecordHeader) = new JoinBuilder {
      override def on(node: Var)(rel: Var): PhysicalResult = {
        val lhsSlot = prev.records.details.header.slotFor(node)
        val rhsSlot = relView.records.details.header.sourceNode(rel)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecordsWithDetails(join(relView.records, header, lhsSlot -> rhsSlot))
      }
    }

    def joinTarget(nodeView: PhysicalResult, header: RecordHeader) = new JoinBuilder {
      override def on(rel: Var)(node: Var): PhysicalResult = {
        val lhsSlot = prev.records.details.header.targetNode(rel)
        val rhsSlot = nodeView.records.details.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecordsWithDetails(join(nodeView.records, header, lhsSlot -> rhsSlot))
      }
    }

    def joinNode(nodeView: PhysicalResult, header: RecordHeader) = new JoinBuilder {
      override def on(endNode: Var)(node: Var): PhysicalResult = {
        val lhsSlot = prev.records.header.slotFor(endNode)
        val rhsSlot = nodeView.records.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecordsWithDetails(join(nodeView.records, header, lhsSlot -> rhsSlot))
      }
    }

    def joinInto(relView: PhysicalResult, header: RecordHeader) = new JoinIntoBuilder {
      override def on(sourceKey: Var, targetKey: Var)(rel: Var): PhysicalResult = {
        val sourceSlot = prev.records.header.slotFor(sourceKey)
        val targetSlot = prev.records.header.slotFor(targetKey)
        val relSourceSlot = relView.records.details.header.sourceNode(rel)
        val relTargetSlot = relView.records.details.header.targetNode(rel)

        assertIsNode(sourceSlot)
        assertIsNode(targetSlot)

        prev.mapRecordsWithDetails(join(relView.records, header,
          sourceSlot -> relSourceSlot,
          targetSlot -> relTargetSlot))
      }
    }

    private def join(rhs: SparkCypherRecords, header: RecordHeader, joinSlots: (RecordSlot, RecordSlot)*)
    : SparkCypherRecords => SparkCypherRecords = {
      def f(lhs: SparkCypherRecords) = {
        if (lhs.space == rhs.space) {
          val lhsData = lhs.details.data
          val rhsData = rhs.details.data

          val joinExpr = joinSlots
            .map { case (l, r) => lhsData.col(context.columnName(l)) === rhsData.col(context.columnName(r)) }
            .reduce(_ && _)
          val jointData = lhsData.join(rhsData, joinExpr, "inner")

          SparkCypherRecords.create(header, jointData)(lhs.space)
        } else {
          Raise.graphSpaceMismatch()
        }
      }
      f
    }

    def initVarExpand(source: Var, edgeList: Var, target: Var, header: RecordHeader): PhysicalResult = {
      val sourceSlot = header.slotFor(source)
      val edgeListSlot = header.slotFor(edgeList)
      val targetSlot = header.slotFor(target)

      assertIsNode(targetSlot)

      prev.mapRecordsWithDetails { subject =>
        val inputData = subject.data
        val keep = inputData.columns.map(inputData.col)

        val edgeListColName = context.columnName(edgeListSlot)
        val edgeListColumn = udf(udfUtils.initArray _, ArrayType(LongType))()
        val withEmptyList = inputData.withColumn(edgeListColName, edgeListColumn)

        val cols = keep ++ Seq(
          withEmptyList.col(edgeListColName),
          inputData.col(context.columnName(sourceSlot)).as(context.columnName(targetSlot)))

        val initializedData = withEmptyList.select(cols: _*)

        SparkCypherRecords.create(header, initializedData)(subject.space)
      }
    }

    def varExpand(rels: PhysicalResult, edgeList: Var, endNode: Var, rel: Var,
                  lower: Int, upper: Int, header: RecordHeader): PhysicalResult = {
      val startSlot = rels.records.details.header.sourceNode(rel)
      val endNodeSlot = prev.records.details.header.slotFor(endNode)

      prev.mapRecordsWithDetails { lhs =>
        val initData = lhs.data
        val relsData = rels.records.details.data

        val edgeListColName = context.columnName(lhs.header.slotFor(edgeList))

        val steps = new mutable.HashMap[Int, DataFrame]
        steps(0) = initData

        val keep = initData.columns

        val listTempColName = FreshVariableNamer.generateUniqueName(lhs.header)

        (1 to upper).foreach { i =>
          // TODO: Check whether we can abort iteration if result has no cardinality (eg count > 0?)
          steps(i) = iterate(steps(i - 1), relsData)(endNodeSlot, rel, startSlot, listTempColName, edgeListColName, keep)
        }

        val union = steps.filterKeys(_ >= lower).values.reduce[DataFrame] {
          case (l, r) => l.union(r)
        }

        SparkCypherRecords.create(lhs.header, union)(lhs.space)
      }
    }

    def finalizeVarExpand(target: PhysicalResult, endNode: Var, targetNode: Var, header: RecordHeader)
    : PhysicalResult = {
      val joinHeader = prev.records.details.header ++ target.records.details.header
      val joined = prev.joinNode(target, joinHeader).on(endNode)(targetNode)

      val endNodeSlot =  prev.records.details.header.slotFor(endNode)
      val endNodeCol = context.columnName(endNodeSlot)
      joined.mapRecordsWithDetails(records =>
        SparkCypherRecords.create(header, records.details.toDF().drop(endNodeCol))(records.space)
      )
    }

    private def iterate(lhs: DataFrame, rels: DataFrame)
                       (endNode: RecordSlot, rel: Var, relStartNode: RecordSlot,
                        listTempColName: String, edgeListColName: String, keep: Array[String]): DataFrame = {

      val relIdColumn = rels.col(context.columnName(OpaqueField(rel)))
      val startColumn = rels.col(context.columnName(relStartNode))
      val expandColumnName = context.columnName(endNode)
      val expandColumn = lhs.col(expandColumnName)

      val joined = lhs.join(rels, expandColumn === startColumn, "inner")

      val appendUdf = udf(udfUtils.arrayAppend _, ArrayType(LongType))
      val extendedArray = appendUdf(lhs.col(edgeListColName), relIdColumn)
      val withExtendedArray = joined.withColumn(listTempColName, extendedArray)
      val arrayContains = udf(udfUtils.contains _, BooleanType)(withExtendedArray.col(edgeListColName), relIdColumn)
      val filtered = withExtendedArray.filter(!arrayContains)

      // TODO: Try and get rid of the Var rel here
      val endNodeIdColNameOfJoinedRel = context.columnName(ProjectedExpr(EndNode(rel)(CTNode)))

      val columns = keep ++ Seq(listTempColName, endNodeIdColNameOfJoinedRel)
      val withoutRelProperties = filtered.select(columns.head, columns.tail: _*)  // drops joined columns from relationship table

      withoutRelProperties
        .drop(expandColumn)
        .withColumnRenamed(endNodeIdColNameOfJoinedRel, expandColumnName)
        .drop(edgeListColName)
        .withColumnRenamed(listTempColName, edgeListColName)
    }

    sealed trait JoinBuilder {
      def on(lhsKey: Var)(rhsKey: Var): PhysicalResult
    }

    sealed trait JoinIntoBuilder {
      def on(sourceKey: Var, targetKey: Var)(rhsKey: Var): PhysicalResult
    }

    private def assertIsNode(slot: RecordSlot): Unit = {
      slot.content.cypherType match {
        case CTNode =>
        case x => throw new IllegalArgumentException(s"Expected $slot to contain a node, but was $x")
      }
    }
  }

  private def liftTernary(f: Row => Option[Boolean]): (Row => Boolean) = {
    (r: Row) =>
      f(r) match {
        case None => false
        case Some(x) => x
      }
  }
}

case object udfUtils {
  def initArray(): Any = {
    Array[Long]()
  }

  def arrayAppend(array: Any, next: Any): Any = {
    array match {
      case a:mutable.WrappedArray[Long] =>
        a :+ next
    }
  }

  def contains(array: Any, elem: Any): Any = {
    array match {
      case a:mutable.WrappedArray[Long] =>
        a.contains(elem)
    }
  }
}

