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
package org.opencypher.caps.impl.spark.physical

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, LongType}
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.types.{CTBoolean, CTNode, CTRelationship}
import org.opencypher.caps.api.value.{CypherInteger, CypherValue}
import org.opencypher.caps.impl.flat.FreshVariableNamer
import org.opencypher.caps.impl.logical.LogicalGraph
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.SparkSQLExprMapper.asSparkSQLExpr
import org.opencypher.caps.impl.spark.convert.toSparkType
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.syntax.expr._
import org.opencypher.caps.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.caps.ir.api.global._
import org.opencypher.caps.ir.api.pattern.{AnyGiven, EveryNode, EveryRelationship}

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

    def nodeScan(inGraph: LogicalGraph, v: Var, labelPredicates: EveryNode, header: RecordHeader)
    : PhysicalResult = {
      val graph = prev.graphs(inGraph.name)

      val records = graph.nodes(v.name, CTNode(labelPredicates.labels.elements.map(_.name).toSeq: _*))

      // TODO: Should not discard prev records here
      prev.mapRecordsWithDetails(_ => records)
    }

    def relationshipScan(inGraph: LogicalGraph, v: Var, typePredicates: EveryRelationship, header: RecordHeader)
    : PhysicalResult = {
      val graph = prev.graphs(inGraph.name)

      val records = graph.relationships(v.name, CTRelationship(typePredicates.relTypes.elements.map(_.name)))

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
            subject.data.filter(predicate)
        }

        val selectedColumns = header.slots.map { c =>
          val name = context.columnName(c)
          filteredRows.col(name)
        }

        val newData = filteredRows.select(selectedColumns: _*)

        CAPSRecords.create(header, newData, CAPSRecordsTokens(context.tokens))(subject.caps)
      }
    }

    def distinct(header: RecordHeader): PhysicalResult = {
      prev.mapRecordsWithDetails { subject =>
        val data: DataFrame = subject.data
        val columnNames = header.slots.map(slot => data.col(context.columnName(slot)))
        val relevantColumns = data.select(columnNames: _*)
        val distinctRows = relevantColumns.distinct()
        CAPSRecords.create(header, distinctRows, CAPSRecordsTokens(context.tokens))(subject.caps)
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

        CAPSRecords.create(header, newData, CAPSRecordsTokens(context.tokens))(subject.caps)
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

        CAPSRecords.create(header, newData, CAPSRecordsTokens(context.tokens))(subject.caps)
      }

    def aggregate(aggregations: Set[(Var, Aggregator)], group: Set[Var], header: RecordHeader): PhysicalResult =
      prev.mapRecordsWithDetails { records =>
        val data = records.data

        def withInnerExpr(expr: Expr)(f: Column => Column) = {
          asSparkSQLExpr(records.header, expr, data) match {
            case None => Raise.notYetImplemented(s"projecting $expr")
            case Some(column) => f(column)
          }
        }

        if (group.nonEmpty)
          Raise.notYetImplemented("grouping")

        val sparkAggFunctions = aggregations.map {
          case (to, Avg(expr)) =>
            withInnerExpr(expr)(functions.avg(_).cast(toSparkType(to.cypherType)).as(to.name))

          case (to, CountStar()) =>
            functions.count(functions.lit(0)).as(to.name)

          // TODO: Consider not implicitly projecting the inner expr here, but rewriting it into a variable in logical planning or IR construction
          case (to, Count(expr)) =>
            withInnerExpr(expr)(functions.count(_).as(to.name))

          case (to, Max(expr)) =>
            withInnerExpr(expr)(functions.max(_).as(to.name))

          case (to, Min(expr)) =>
            withInnerExpr(expr)(functions.min(_).as(to.name))

          case (to, Sum(expr)) =>
            withInnerExpr(expr)(functions.sum(_).as(to.name))

          case x =>
            Raise.notYetImplemented(s"Aggregator $x")
        }

        CAPSRecords.create(header, data.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*), CAPSRecordsTokens(context.tokens))(records.caps)
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

        CAPSRecords.create(header, newData, CAPSRecordsTokens(context.tokens))(subject.caps)
      }

    def typeFilter(rel: Var, types: AnyGiven[RelType], header: RecordHeader): PhysicalResult = {
      if (types.elements.isEmpty) prev
      else {
        val typeExprs: Set[Expr] = types.elements.map { ref => HasType(rel, ref)(CTBoolean) }
        prev.filter(Ors(typeExprs), header)
      }
    }

    def orderBy(sortItems: Seq[SortItem[Expr]], header: RecordHeader): PhysicalResult = {

      val getColumnName = (expr: Var) => context.columnName(prev.records.details.header.slotFor(expr))

      val sortExpression = sortItems.map {
        case Asc(expr: Var) => asc(getColumnName(expr))
        case Desc(expr: Var) => desc(getColumnName(expr))
        case _ => Raise.impossible()
      }

      prev.mapRecordsWithDetails { subject =>
        val sortedData = subject.details.toDF().sort(sortExpression: _*)
        CAPSRecords.create(header, sortedData, CAPSRecordsTokens(context.tokens))(subject.caps)
      }
    }

    def skip(expr: Expr, header: RecordHeader): PhysicalResult = {
      val skip: Long = expr match {
        case IntegerLit(v) => v
        case Const(constant) =>
          context.parameters(context.constants.constantRef(constant)) match {
            case CypherInteger(v) => v
            case _ => Raise.impossible()
          }
        case _ => Raise.impossible()
      }

      // TODO: Replace with data frame based implementation ASAP
      prev.mapRecordsWithDetails { subject =>
        val newDf = subject.caps.sparkSession.createDataFrame(
          subject.details.toDF().rdd.zipWithIndex().filter((pair) => pair._2 >= skip).map(_._1),
          subject.details.toDF().schema
        )
        CAPSRecords.create(header, newDf, CAPSRecordsTokens(context.tokens))(subject.caps)
      }
    }

    def limit(expr: Expr, header: RecordHeader): PhysicalResult = {
      val limit = expr match {
        case IntegerLit(v) => v
        case _ => Raise.impossible()
      }

      prev.mapRecordsWithDetails { subject =>
        CAPSRecords.create(header, subject.details.toDF().limit(limit.toInt), CAPSRecordsTokens(context.tokens))(subject.caps)
      }
    }

    def joinSource(relView: PhysicalResult, header: RecordHeader) = new JoinBuilder {
      override def on(node: Var)(rel: Var): PhysicalResult = {
        val lhsSlot = prev.records.details.header.slotFor(node)
        val rhsSlot = relView.records.details.header.sourceNodeSlot(rel)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecordsWithDetails(join(relView.records, header, lhsSlot -> rhsSlot)())
      }
    }

    def joinTarget(nodeView: PhysicalResult, header: RecordHeader) = new JoinBuilder {
      override def on(rel: Var)(node: Var): PhysicalResult = {
        val lhsSlot = prev.records.details.header.targetNodeSlot(rel)
        val rhsSlot = nodeView.records.details.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecordsWithDetails(join(nodeView.records, header, lhsSlot -> rhsSlot)())
      }
    }

    def joinNode(nodeView: PhysicalResult, header: RecordHeader) = new JoinBuilder {
      override def on(endNode: Var)(node: Var): PhysicalResult = {
        val lhsSlot = prev.records.header.slotFor(endNode)
        val rhsSlot = nodeView.records.header.slotFor(node)

        assertIsNode(lhsSlot)
        assertIsNode(rhsSlot)

        prev.mapRecordsWithDetails(join(nodeView.records, header, lhsSlot -> rhsSlot)())
      }
    }

    def joinInto(relView: PhysicalResult, header: RecordHeader) = new JoinIntoBuilder {
      override def on(sourceKey: Var, targetKey: Var)(rel: Var): PhysicalResult = {
        val sourceSlot = prev.records.header.slotFor(sourceKey)
        val targetSlot = prev.records.header.slotFor(targetKey)
        val relSourceSlot = relView.records.details.header.sourceNodeSlot(rel)
        val relTargetSlot = relView.records.details.header.targetNodeSlot(rel)

        assertIsNode(sourceSlot)
        assertIsNode(targetSlot)

        prev.mapRecordsWithDetails(join(relView.records, header,
          sourceSlot -> relSourceSlot,
          targetSlot -> relTargetSlot)())
      }
    }

    def optional(rhs: PhysicalResult, lhsHeader: RecordHeader, rhsHeader: RecordHeader): PhysicalResult = {
      val commonFields = rhsHeader.fields.intersect(lhsHeader.fields)
      val rhsData = rhs.records.details.toDF()
      val lhsData = prev.records.details.toDF()

      // Remove all common columns from the right hand side, except the join columns
      val columnsToRemove = commonFields
        .flatMap(rhsHeader.childSlots)
        .map(_.content)
        .map(context.columnName).toSeq

      val lhsJoinSlots = commonFields.map(lhsHeader.slotFor)
      val rhsJoinSlots = commonFields.map(rhsHeader.slotFor)

      // Find the join pairs and introduce an alias for the right hand side
      // This is necessary to be able to deduplicate the join columns later
      val joinColumnMapping = lhsJoinSlots
        .map(lhsSlot => {
          lhsSlot -> rhsJoinSlots.find(_.content == lhsSlot.content).get
        })
        .map(pair => {
          val lhsCol = lhsData.col(context.columnName(pair._1))
          val rhsColName = context.columnName(pair._2)

          (lhsCol, rhsColName, FreshVariableNamer.generateUniqueName(rhsHeader))
        }).toSeq

      val reducedRhsData = joinColumnMapping
        .foldLeft(rhsData)((acc, col) => acc.withColumnRenamed(col._2, col._3))
        .drop(columnsToRemove: _*)

      val joinCols = joinColumnMapping.map(t => t._1 -> reducedRhsData.col(t._3))

      prev.mapRecordsWithDetails(join(reducedRhsData, rhsHeader, joinCols: _*)("leftouter", deduplicate = true))
    }

    private def join(rhs: CAPSRecords, header: RecordHeader, joinSlots: (RecordSlot, RecordSlot)*)
                    (joinType: String = "inner", deduplicate: Boolean = false)
                    : CAPSRecords => CAPSRecords = {

      val lhsData = prev.records.details.toDF()
      val rhsData = rhs.details.toDF()

      val joinCols = joinSlots.map(pair =>
        lhsData.col(context.columnName(pair._1)) -> rhsData.col(context.columnName(pair._2))
      )

      join(rhsData, header, joinCols: _*)(joinType, deduplicate)
    }

    private def join(rhsData: DataFrame, header: RecordHeader, joinCols: (Column, Column)*)
                    (joinType: String, deduplicate: Boolean): CAPSRecords => CAPSRecords = {

      def f(lhs: CAPSRecords) = {
        val lhsData = lhs.details.data

        val joinExpr = joinCols
          .map { case (l, r) => l === r }
          .reduce(_ && _)

        val joinedData = lhsData.join(rhsData, joinExpr, joinType)

        val returnData = if (deduplicate) {
          val colsToDrop = joinCols.map(col => col._2)
          colsToDrop.foldLeft(joinedData)((acc, col) => acc.drop(col))
        } else joinedData

        CAPSRecords.create(header, returnData, CAPSRecordsTokens(context.tokens))(lhs.caps)
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

        CAPSRecords.create(header, initializedData, CAPSRecordsTokens(context.tokens))(subject.caps)
      }
    }

    def varExpand(rels: PhysicalResult, edgeList: Var, endNode: Var, rel: Var,
                  lower: Int, upper: Int, header: RecordHeader): PhysicalResult = {
      val startSlot = rels.records.details.header.sourceNodeSlot(rel)
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

        CAPSRecords.create(lhs.header, union, CAPSRecordsTokens(context.tokens))(lhs.caps)
      }
    }

    def finalizeVarExpand(target: PhysicalResult, endNode: Var, targetNode: Var, header: RecordHeader)
    : PhysicalResult = {
      val joinHeader = prev.records.details.header ++ target.records.details.header
      val joined = prev.joinNode(target, joinHeader).on(endNode)(targetNode)

      val endNodeSlot =  prev.records.details.header.slotFor(endNode)
      val endNodeCol = context.columnName(endNodeSlot)
      joined.mapRecordsWithDetails(records =>
        CAPSRecords.create(header, records.details.toDF().drop(endNodeCol), CAPSRecordsTokens(context.tokens))(records.caps)
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
        case CTNode(_) =>
        case x => throw new IllegalArgumentException(s"Expected $slot to contain a node, but was $x")
      }
    }
  }
}

case object udfUtils {
  def initArray(): Any = {
    Array[Long]()
  }

  def arrayAppend(array: Any, next: Any): Any = {
    array match {
      case a: mutable.WrappedArray[_] =>
        a :+ next
    }
  }

  def contains(array: Any, elem: Any): Any = {
    array match {
      case a: mutable.WrappedArray[_] =>
        a.contains(elem)
    }
  }
}

