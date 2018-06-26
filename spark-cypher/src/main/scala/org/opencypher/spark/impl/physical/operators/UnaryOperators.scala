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
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.impl.physical.{Ascending, Descending, Order}
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.spark.api.io.SparkCypherTable
import org.opencypher.spark.api.io.SparkCypherTable.DataFrameTable
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.CAPSUnionGraph.{apply => _, unapply => _}
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.impl.convert.SparkConversions._

final case class Cache(in: CAPSPhysicalOperator) extends CAPSPhysicalOperator {

  override lazy val table: DataFrameTable = context.cache.getOrElse(in, {
    in.table.cache()
    context.cache(in) = in.table
    in.table
  })

}

final case class NodeScan(in: CAPSPhysicalOperator, v: Var) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.graph.schema.headerForNode(v)

  // TODO: replace with NodeVar
  override lazy val table: DataFrameTable = in.graph.nodes(v.name, v.cypherType.asInstanceOf[CTNode]).table

}

final case class RelationshipScan(in: CAPSPhysicalOperator, v: Var) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.graph.schema.headerForRelationship(v)

  // TODO: replace with RelationshipVar
  override lazy val table: DataFrameTable = in.graph.relationships(v.name, v.cypherType.asInstanceOf[CTRelationship]).table
}

final case class Alias(in: CAPSPhysicalOperator, aliases: Seq[AliasExpr]) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.header.withAlias(aliases: _*)
}

final case class AddColumn(in: CAPSPhysicalOperator, expr: Expr) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = {
    if (in.header.contains(expr)) {
      expr match {
        case a: AliasExpr => in.header.withAlias(a)
        case _ => in.header
      }
    } else {
      expr match {
        case a: AliasExpr => in.header.withExpr(a.expr).withAlias(a)
        case _ => in.header.withExpr(expr)
      }
    }
  }

  override lazy val table: DataFrameTable = {
    if (in.header.contains(expr)) {
      in.table
    } else {
      in.table.withColumn(header.column(expr), expr)(header, context.parameters)
    }
  }
}

final case class CopyColumn(in: CAPSPhysicalOperator, from: Expr, to: Expr) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.header.withExpr(to)

  override lazy val table: DataFrameTable = in.table.withColumn(header.column(to), from)(header, context.parameters)
}

final case class DropColumns[T <: Expr](in: CAPSPhysicalOperator, exprs: Set[T]) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = header -- exprs

  override lazy val table: DataFrameTable = {
    if (header.columns.size < in.header.columns.size) {
      in.table.drop(exprs.map(header.column).toSeq: _*)
    } else {
      in.table
    }
  }
}

final case class RenameColumns(in: CAPSPhysicalOperator, renameExprs: Map[Expr, String]) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = renameExprs.foldLeft(in.header) {
    case (currentHeader, (expr, newColumn)) => currentHeader.withColumnRenamed(expr, newColumn)
  }

  override lazy val table: DataFrameTable = renameExprs.foldLeft(in.table) {
    case (currentTable, (expr, newColumn)) => currentTable.withColumnRenamed(header.column(expr), newColumn)
  }
}

final case class Filter(in: CAPSPhysicalOperator, expr: Expr) extends CAPSPhysicalOperator {

  override lazy val table: DataFrameTable = in.table.filter(expr)(header, context.parameters)
}

final case class ReturnGraph(in: CAPSPhysicalOperator) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = RecordHeader.empty

  override lazy val table: DataFrameTable = in.table.empty()
}

final case class Select(in: CAPSPhysicalOperator, expressions: List[Expr]) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = {
    val aliasExprs = expressions.collect { case a: AliasExpr => a }
    val headerWithAliases = header.withAlias(aliasExprs: _*)
    headerWithAliases.select(expressions: _*)
  }

  override lazy val table: SparkCypherTable.DataFrameTable = {
    in.table.select(expressions.map(header.column).distinct: _*)
  }

  override lazy val returnItems: Option[Seq[Var]] = Some(expressions.collect { case e: Var => e })
}

final case class Distinct(in: CAPSPhysicalOperator, fields: Set[Var]) extends CAPSPhysicalOperator {

  override lazy val table: DataFrameTable = in.table.distinct(fields.flatMap(header.expressionsFor).map(header.column).toSeq: _*)

}

final case class SimpleDistinct(in: CAPSPhysicalOperator) extends CAPSPhysicalOperator {

  override lazy val table: DataFrameTable = in.table.distinct
}

final case class Aggregate(
  in: CAPSPhysicalOperator,
  aggregations: Set[(Var, Aggregator)],
  group: Set[Var]
) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.header.select(group).withExprs(aggregations.map(_._1))

  override lazy val table: DataFrameTable = {

    val inDF = in.table.df

    def withInnerExpr(expr: Expr)(f: Column => Column) =
      f(expr.asSparkSQLExpr(in.header, inDF, context.parameters))

    val data: Either[RelationalGroupedDataset, DataFrame] =
      if (group.nonEmpty) {
        val columns = group.flatMap { expr =>
          val withChildren = in.header.ownedBy(expr)
          withChildren.map(e => withInnerExpr(e)(identity))
        }
        Left(inDF.groupBy(columns.toSeq: _*))
      } else Right(inDF)

    val sparkAggFunctions = aggregations.map {
      case (to, inner) =>
        val columnName = header.column(to)
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

    data.fold(
      _.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*),
      _.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*)
    )
  }
}

final case class OrderBy(in: CAPSPhysicalOperator, sortItems: Seq[SortItem[Expr]]) extends CAPSPhysicalOperator {

  override lazy val table: DataFrameTable = {
    val tableSortItems: Seq[(String, Order)] = sortItems.map {
      case Asc(expr) => header.column(expr) -> Ascending
      case Desc(expr) => header.column(expr) -> Descending
    }
    table.orderBy(tableSortItems: _*)
  }
}

final case class Skip(in: CAPSPhysicalOperator, expr: Expr) extends CAPSPhysicalOperator {

  override lazy val table: DataFrameTable = {
    val skip: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(l) => l
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal or parameter", other)
    }
    in.table.skip(skip)
  }
}

final case class Limit(in: CAPSPhysicalOperator, expr: Expr) extends CAPSPhysicalOperator {

  override lazy val table: DataFrameTable = {
    val limit: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(v) => v
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal", other)
    }
    in.table.limit(limit)
  }
}

final case class EmptyRecords(in: CAPSPhysicalOperator, fields: Set[Var] = Set.empty) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = RecordHeader.from(fields)

  override lazy val table: DataFrameTable = in.table.empty(header)
}

final case class FromGraph(in: CAPSPhysicalOperator, logicalGraph: LogicalCatalogGraph) extends CAPSPhysicalOperator {

  override def graph: CAPSGraph = resolve(logicalGraph.qualifiedGraphName)

  override def graphName: QualifiedGraphName = logicalGraph.qualifiedGraphName

}
