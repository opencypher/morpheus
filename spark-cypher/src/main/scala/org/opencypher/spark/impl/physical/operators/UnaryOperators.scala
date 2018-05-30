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

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{asc, desc}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException, SchemaException}
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.CAPSUnionGraph.{apply => _, unapply => _}
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.impl.convert.CAPSCypherType._
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator._
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}

private[spark] abstract class UnaryPhysicalOperator extends CAPSPhysicalOperator {

  def in: CAPSPhysicalOperator

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    executeUnary(in.execute)
  }

  def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult
}

final case class Cache(in: CAPSPhysicalOperator) extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    context.cache.getOrElse(in, {
      prev.records.cache()
      context.cache(in) = prev
      prev
    })
  }
}

final case class NodeScan(in: CAPSPhysicalOperator, v: Var, header: RecordHeader)
  extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val graph = prev.workingGraph
    val records = v.cypherType match {
      case n: CTNode => graph.nodes(v.name, n)
      case other => throw IllegalArgumentException("Node variable", other)
    }
    if (header != records.header) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed record header based on graph schema: ${header.slots.map(_.content).mkString("[", ",", "]")}
           |  - Actual record header: ${records.header.slots.map(_.content).mkString("[", ",", "]")}
        """.stripMargin)
    }
    CAPSPhysicalResult(records, graph, prev.workingGraphName)
  }
}

final case class RelationshipScan(
  in: CAPSPhysicalOperator,
  v: Var,
  header: RecordHeader
) extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val graph = prev.workingGraph
    val records = v.cypherType match {
      case r: CTRelationship => graph.relationships(v.name, r)
      case other => throw IllegalArgumentException("Relationship variable", other)
    }
    if (header != records.header) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed record header based on graph schema: ${header.slots.map(_.content).mkString("[", ",", "]")}
           |  - Actual record header: ${records.header.slots.map(_.content).mkString("[", ",", "]")}
        """.stripMargin)
    }
    CAPSPhysicalResult(records, graph, v.cypherType.graph.get)
  }
}

final case class Alias(in: CAPSPhysicalOperator, aliases: Seq[(Expr, Var)], header: RecordHeader)
  extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val inHeader = records.header

      val newData = aliases.foldLeft(records.data) {
        case (acc, (expr, alias)) =>
          val oldSlot = inHeader.slotsFor(expr).head
          val newSlot = header.slotsFor(alias).head

          val oldColumnName = inHeader.of(oldSlot)
          val newColumnName = header.of(newSlot)

          if (records.data.columns.contains(oldColumnName)) {
            acc.safeRenameColumn(oldColumnName, newColumnName)
          } else {
            throw IllegalArgumentException(s"a column with name $oldColumnName")
          }
      }

      CAPSRecords.verifyAndCreate(header, newData)(records.caps)
    }
  }
}

final case class Project(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader)
  extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val headerNames = header.slotsFor(expr).map(header.of)
      val dataNames = records.data.columns.toSeq

      // TODO: Can optimise for var AS var2 case -- avoid duplicating data
      val newData = headerNames.diff(dataNames) match {
        case Seq(one) =>
          // align the name of the column to what the header expects
          val newCol = expr.asSparkSQLExpr(header, records.data, context).as(one)
          val columnsToSelect = records.data.columns
            .map(records.data.col) :+ newCol

          records.data.select(columnsToSelect: _*)
        case seq if seq.isEmpty => throw IllegalStateException(s"Did not find a slot for expression $expr in $headerNames")
        case seq => throw IllegalStateException(s"Got multiple slots for expression $expr: $seq")
      }

      CAPSRecords.verifyAndCreate(header, newData)(records.caps)
    }
  }
}

final case class Drop(
  in: CAPSPhysicalOperator,
  dropFields: Seq[Expr],
  header: RecordHeader
) extends UnaryPhysicalOperator with PhysicalOperatorDebugging {
  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val columnToDrop = dropFields
        .map(records.header.of)
        .filter(records.data.columns.contains)

      val withDropped = records.data.safeDropColumns(columnToDrop: _*)
      CAPSRecords.verifyAndCreate(header, withDropped)(records.caps)
    }
  }
}

final case class Filter(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader)
  extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val filteredRows = records.data.where(expr.asSparkSQLExpr(header, records.data, context))
      CAPSRecords.verifyAndCreate(header, filteredRows)(records.caps)
    }
  }
}

final case class ReturnGraph(in: CAPSPhysicalOperator)
  extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    CAPSPhysicalResult(CAPSRecords.empty(header)(prev.records.caps), prev.workingGraph, prev.workingGraphName)
  }

  override def header: RecordHeader = RecordHeader.empty

}

final case class Select(in: CAPSPhysicalOperator, expressions: List[(Expr, Option[Var])], header: RecordHeader)
  extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>

      val data = records.data

      val columns = expressions.map {
        case (expr, alias) =>
          val column = expr.asSparkSQLExpr(header, prev.records.data, context)
          if (alias.isDefined) {
            val aliasName = header.of(header.slotsFor(alias.get).head)
            column.as(aliasName)
          } else {
            val aliasName = header.of(header.slotsFor(expr).head)
            column.as(aliasName)
          }

      }

      val newData = data.select(columns: _*)

      CAPSRecords.verifyAndCreate(header, newData)(records.caps)
    }
  }
}

final case class Distinct(in: CAPSPhysicalOperator, fields: Set[Var])
  extends UnaryPhysicalOperator with InheritedHeader with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val data = records.data
      val relevantColumns = fields.map(header.slotFor).map(records.header.of)
      val distinctRows = data.dropDuplicates(relevantColumns.toSeq)
      CAPSRecords.verifyAndCreate(header, distinctRows)(records.caps)
    }
  }
}

final case class SimpleDistinct(in: CAPSPhysicalOperator)
  extends UnaryPhysicalOperator with InheritedHeader with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      CAPSRecords.verifyAndCreate(prev.records.header, records.data.distinct())(records.caps)
    }
  }
}

final case class Aggregate(
  in: CAPSPhysicalOperator,
  aggregations: Set[(Var, Aggregator)],
  group: Set[Var],
  header: RecordHeader
) extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val inData = records.data

      def withInnerExpr(expr: Expr)(f: Column => Column) =
        f(expr.asSparkSQLExpr(records.header, inData, context))

      val data: Either[RelationalGroupedDataset, DataFrame] =
        if (group.nonEmpty) {
          val columns = group.flatMap { expr =>
            val withChildren = records.header.selfWithChildren(expr).map(_.content.key)
            withChildren.map(e => withInnerExpr(e)(identity))
          }
          Left(inData.groupBy(columns.toSeq: _*))
        } else Right(inData)

      val sparkAggFunctions = aggregations.map {
        case (to, inner) =>
          val columnName = header.of(to)
          inner match {
            case Avg(expr) =>
              withInnerExpr(expr)(
                functions
                  .avg(_)
                  .cast(to.cypherType.getSparkType)
                  .as(columnName))

            case CountStar(_) =>
              functions.count(functions.lit(0)).as(columnName)

            // TODO: Consider not implicitly projecting the inner expr here, but rewriting it into a variable in logical planning or IR construction
            case Count(expr, distinct) => withInnerExpr(expr) { column =>
              val count = {
                if (distinct) functions.countDistinct(column)
                else functions.count(column)
              }

              count.as(columnName)
            }

            case Max(expr) =>
              withInnerExpr(expr)(functions.max(_).as(columnName))

            case Min(expr) =>
              withInnerExpr(expr)(functions.min(_).as(columnName))

            case Sum(expr) =>
              withInnerExpr(expr)(functions.sum(_).as(columnName))

            case Collect(expr, distinct) => withInnerExpr(expr) { column =>
              val list = {
                if (distinct) functions.collect_set(column)
                else functions.collect_list(column)
              }
              // sort for deterministic aggregation results
              val sorted = functions.sort_array(list)
              sorted.as(columnName)
            }

            case x =>
              throw NotImplementedException(s"Aggregation function $x")
          }
      }

      val aggregated = data.fold(
        _.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*),
        _.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*)
      )

      CAPSRecords.verifyAndCreate(header, aggregated)(records.caps)
    }
  }
}

final case class OrderBy(in: CAPSPhysicalOperator, sortItems: Seq[SortItem[Expr]])
  extends UnaryPhysicalOperator with InheritedHeader with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val getColumnName = (expr: Var) => prev.records.header.of(prev.records.header.slotFor(expr))

    val sortExpression = sortItems.map {
      case Asc(expr: Var) => asc(getColumnName(expr))
      case Desc(expr: Var) => desc(getColumnName(expr))
      case other => throw IllegalArgumentException("ASC or DESC", other)
    }

    prev.mapRecordsWithDetails { records =>
      val sortedData = records.toDF().sort(sortExpression: _*)
      CAPSRecords.verifyAndCreate(header, sortedData)(records.caps)
    }
  }
}

final case class Skip(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader)
  extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val skip: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(l) => l
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal or parameter", other)
    }

    // TODO: Replace with data frame based implementation ASAP
    prev.mapRecordsWithDetails { records =>
      val newDf = records.caps.sparkSession.createDataFrame(
        records
          .toDF()
          .rdd
          .zipWithIndex()
          .filter((pair) => pair._2 >= skip)
          .map(_._1),
        records.toDF().schema
      )
      CAPSRecords.verifyAndCreate(header, newDf)(records.caps)
    }
  }
}

final case class Limit(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader)
  extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val limit: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(v) => v
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal", other)
    }

    prev.mapRecordsWithDetails { records =>
      CAPSRecords.verifyAndCreate(header, records.toDF().limit(limit.toInt))(records.caps)
    }
  }
}

// Initialises the table in preparation for variable length expand.
final case class InitVarExpand(in: CAPSPhysicalOperator, source: Var, edgeList: Var, target: Var, header: RecordHeader)
  extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val sourceSlot = header.slotFor(source)
    val edgeListSlot = header.slotFor(edgeList)
    val targetSlot = header.slotFor(target)

    assertIsNode(targetSlot)

    prev.mapRecordsWithDetails { records =>
      val inputData = records.data
      val keep = inputData.columns.map(inputData.col)

      val edgeListColName = header.of(edgeListSlot)
      val edgeListColumn = functions.typedLit(Array[Long]())
      val withEmptyList = inputData.safeAddColumn(edgeListColName, edgeListColumn)

      val cols = keep ++ Seq(
        withEmptyList.col(edgeListColName),
        inputData.col(header.of(sourceSlot)).as(header.of(targetSlot)))

      val initializedData = withEmptyList.select(cols: _*)

      CAPSRecords.verifyAndCreate(header, initializedData)(records.caps)
    }
  }
}

final case class EmptyRecords(in: CAPSPhysicalOperator, header: RecordHeader)(implicit caps: CAPSSession)
  extends UnaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult =
    prev.mapRecordsWithDetails(_ => CAPSRecords.empty(header))

}

final case class FromGraph(
  in: CAPSPhysicalOperator,
  graph: LogicalCatalogGraph
) extends UnaryPhysicalOperator with InheritedHeader with PhysicalOperatorDebugging {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    CAPSPhysicalResult(prev.records, resolve(graph.qualifiedGraphName), graph.qualifiedGraphName)
  }

}
