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
import org.apache.spark.sql.types.{StructField, StructType}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.impl.syntax.ExprSyntax._
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.impl.table.RecordHeader._
import org.opencypher.spark.impl.table.CAPSRecordHeader._
import org.opencypher.okapi.relational.impl.table.{ColumnName, _}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.CAPSUnionGraph.{apply => _, unapply => _}
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.impl.convert.CAPSCypherType._
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator._
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}

import scala.collection.immutable

private[spark] abstract class UnaryPhysicalOperator extends CAPSPhysicalOperator {

  def in: CAPSPhysicalOperator

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = executeUnary(in.execute)

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

final case class NodeScan(in: CAPSPhysicalOperator, v: Var, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val graph = prev.workingGraph
    val records = v.cypherType match {
      case n: CTNode => graph.nodes(v.name, n)
      case other => throw IllegalArgumentException("Node variable", other)
    }
    assert(header == records.header)
    CAPSPhysicalResult(records, graph, prev.workingGraphName)
  }
}

final case class RelationshipScan(in: CAPSPhysicalOperator, v: Var, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val graph = prev.workingGraph
    val records = v.cypherType match {
      case r: CTRelationship => graph.relationships(v.name, r)
      case other => throw IllegalArgumentException("Relationship variable", other)
    }
    assert(header == records.header)
    CAPSPhysicalResult(records, graph, v.cypherType.graph.get)
  }
}

final case class Unwind(in: CAPSPhysicalOperator, list: Expr, item: Var, header: RecordHeader)
  extends UnaryPhysicalOperator {

  import scala.collection.JavaConverters._

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val itemColumn = header.exprFor(item).columnName
      val newData = list match {
        // the list is external: we create a dataframe and crossjoin with it
        case Param(name) =>
          // we need a Java list of rows to construct a DataFrame
          context.parameters(name).as[CypherList] match {
            case Some(l) =>
              val sparkType = item.cypherType.getSparkType
              val nullable = item.cypherType.isNullable
              val schema = StructType(Seq(StructField(itemColumn, sparkType, nullable)))

              val javaRowList = l.unwrap.map(Row(_)).asJava
              val df = records.caps.sparkSession.createDataFrame(javaRowList, schema)

              records.data.crossJoin(df)

            case None =>
              throw IllegalArgumentException("a list", list)
          }

        // the list lives in a column: we explode it
        case expr =>
          val listColumn = expr.asSparkSQLExpr(records.header, records.data, context)

          records.data.safeAddColumn(itemColumn, functions.explode(listColumn))
      }

      CAPSRecords.verifyAndCreate(header, newData)(records.caps)
    }
  }
}

final case class Alias(in: CAPSPhysicalOperator, expr: Expr, alias: Var, header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val oldSlot = records.header.exprFor(expr)

      val newSlot = header.exprFor(alias)

      val oldColumnName = oldSlot.columnName
      val newColumnName = newSlot.columnName

      val newData = if (records.data.columns.contains(oldColumnName)) {
        records.data.safeRenameColumn(oldColumnName, newColumnName)
      } else {
        throw IllegalArgumentException(s"a column with name $oldColumnName")
      }

      CAPSRecords.verifyAndCreate(header, newData)(records.caps)
    }
  }
}

final case class Project(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val sparkColumnNameForExpr = header.exprFor(expr).columnName
      val sparkColumnNames = records.data.columns

      if (sparkColumnNames.contains(sparkColumnNameForExpr)) {
        // Already projected, was aliased in header. Do nothing.
        records
      } else {
        val newData = records.data.safeAddColumn(
          sparkColumnNameForExpr, expr.asSparkSQLExpr(header, records.data, context)
        )
        CAPSRecords.verifyAndCreate(header, newData)(records.caps)
      }
    }

  }
}

final case class Filter(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val filteredRows = records.data.where(expr.asSparkSQLExpr(header, records.data, context))
      CAPSRecords.verifyAndCreate(header, filteredRows)(records.caps)
    }
  }
}

final case class RemoveAliases(
  in: CAPSPhysicalOperator,
  dependentFields: Set[(Expr, Expr)],
  header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val renamed = dependentFields.foldLeft(records.data) {
        case (df, (v, expr)) =>
          df.safeRenameColumn(v.columnName, expr.columnName)
      }

      CAPSRecords.verifyAndCreate(header, renamed)(records.caps)
    }
  }
}

final case class ReturnGraph(in: CAPSPhysicalOperator) extends UnaryPhysicalOperator {
  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    CAPSPhysicalResult(CAPSRecords.empty(header)(prev.records.caps), prev.workingGraph, prev.workingGraphName)
  }

  override def header: RecordHeader = RecordHeader.empty
}

final case class SelectFields(in: CAPSPhysicalOperator, fields: List[Var], header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val columnNamesToSelect = fields.flatMap(header.exprFor(_).expr).map(_.columnName).distinct
      val newData = records.data.select(columnNamesToSelect.map(records.data.col): _*)

      CAPSRecords.verifyAndCreate(header, newData)(records.caps)
    }
  }
}

final case class Distinct(in: CAPSPhysicalOperator, fields: Set[Var])
  extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val data = records.data
      val relevantColumns = fields.map(header.exprFor).map(ColumnName.of)
      val distinctRows = data.dropDuplicates(relevantColumns.toSeq)
      CAPSRecords.verifyAndCreate(header, distinctRows)(records.caps)
    }
  }
}

final case class SimpleDistinct(in: CAPSPhysicalOperator)
  extends UnaryPhysicalOperator with InheritedHeader {

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
  header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val inData = records.data

      def withInnerExpr(expr: Expr)(f: Column => Column) =
        f(expr.asSparkSQLExpr(records.header, inData, context))

      val data: Either[RelationalGroupedDataset, DataFrame] =
        if (group.nonEmpty) {
          val columns = group.flatMap { expr =>
            val withChildren = records.header.selectField(expr).map(_.expr)
            withChildren.map(e => withInnerExpr(e)(identity))
          }
          Left(inData.groupBy(columns.toSeq: _*))
        } else Right(inData)

      val sparkAggFunctions = aggregations.map {
        case (to, inner) =>
          val columnName = ColumnName.from(to.name)
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
  extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val getColumnName = (expr: Var) => ColumnName.of(prev.records.header.exprFor(expr))

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

final case class Skip(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

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

final case class Limit(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

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
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val sourceSlot = header.exprFor(source)
    val edgeListSlot = header.exprFor(edgeList)
    val targetSlot = header.exprFor(target).expr

    assertIsNode(targetSlot)

    prev.mapRecordsWithDetails { records =>
      val inputData = records.data
      val keep = inputData.columns.map(inputData.col)

      val edgeListColName = edgeListSlot.columnName
      val edgeListColumn = functions.typedLit(Array[Long]())
      val withEmptyList = inputData.safeAddColumn(edgeListColName, edgeListColumn)

      val cols = keep ++ Seq(
        withEmptyList.col(edgeListColName),
        inputData.col(sourceSlot.columnName).as(targetSlot.columnName))

      val initializedData = withEmptyList.select(cols: _*)

      CAPSRecords.verifyAndCreate(header, initializedData)(records.caps)
    }
  }
}

final case class EmptyRecords(in: CAPSPhysicalOperator, header: RecordHeader)(implicit caps: CAPSSession)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult =
    prev.mapRecordsWithDetails(_ => CAPSRecords.empty(header))

}

final case class FromGraph(in: CAPSPhysicalOperator, graph: LogicalCatalogGraph) extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    CAPSPhysicalResult(prev.records, resolve(graph.qualifiedGraphName), graph.qualifiedGraphName)
  }

}
