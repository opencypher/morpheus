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

import java.net.URI

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{asc, desc, monotonically_increasing_id, udf}
import org.apache.spark.sql.types.{ArrayType, LongType, StructField, StructType}
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CypherInteger
import org.opencypher.caps.impl.record._
import org.opencypher.caps.impl.spark.SparkSQLExprMapper.asSparkSQLExpr
import org.opencypher.caps.impl.spark.convert.toSparkType
import org.opencypher.caps.impl.spark.physical.operators.PhysicalOperator.{assertIsNode, columnName}
import org.opencypher.caps.impl.spark.physical.{PhysicalResult, RuntimeContext, cypherFilter, udfUtils}
import org.opencypher.caps.impl.spark.{CAPSGraph, CAPSRecords, SparkColumnName}
import org.opencypher.caps.impl.syntax.RecordHeaderSyntax._
import org.opencypher.caps.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.impl.convert.toJava
import org.opencypher.caps.ir.impl.syntax.ExprSyntax._
import org.opencypher.caps.logical.impl.{ConstructedEntity, _}

private[spark] abstract class UnaryPhysicalOperator extends PhysicalOperator {

  def in: PhysicalOperator

  override def execute(implicit context: RuntimeContext): PhysicalResult = executeUnary(in.execute)

  def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult
}

final case class Cache(in: PhysicalOperator) extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    context.cache.getOrElse(in, {
      prev.records.cache()
      context.cache(in) = prev
      prev
    })
  }

}

final case class Scan(in: PhysicalOperator, inGraph: LogicalGraph, v: Var, header: RecordHeader)
    extends UnaryPhysicalOperator {

  // TODO: Move to Graph interface?
  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    val graphs = prev.graphs
    val graph = graphs(inGraph.name)
    val records = v.cypherType match {
      case r: CTRelationship =>
        graph.relationships(v.name, r)
      case n: CTNode =>
        graph.nodes(v.name, n)
      case x =>
        throw IllegalArgumentException("an entity type", x)
    }
    assert(header == records.header)
    PhysicalResult(records, graphs)
  }
}

final case class Unwind(in: PhysicalOperator, list: Expr, item: Var, header: RecordHeader)
    extends UnaryPhysicalOperator {
  import scala.collection.JavaConverters._

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val itemColumn = columnName(header.slotFor(item))
      val newData = list match {
        // the list is external: we create a dataframe and crossjoin with it
        case Param(name) =>
          // we need a Java list of rows to construct a DataFrame
          toJava(context.parameters(name)) match {
            case t: TraversableOnce[_] =>
              val list = t.map(Row(_)).toList.asJava

              val sparkType = toSparkType(item.cypherType)
              val nullable = item.cypherType.isNullable
              val schema = StructType(Seq(StructField(itemColumn, sparkType, nullable)))

              val df = records.caps.sparkSession.createDataFrame(list, schema)

              records.data.crossJoin(df)

            case x =>
              throw IllegalArgumentException("a list", x)
          }

        // the list lives in a column: we explode it
        case expr =>
          val listColumn = asSparkSQLExpr(records.header, expr, records.data) match {
            case Some(c) => c
            case None    => throw IllegalArgumentException(s"a column for the list $expr")
          }

          records.data.withColumn(itemColumn, functions.explode(listColumn))
      }

      CAPSRecords.create(header, newData)(records.caps)
    }
  }
}

final case class Alias(in: PhysicalOperator, expr: Expr, alias: Var, header: RecordHeader)
    extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val oldSlot = records.header.slotsFor(expr).head

      val newSlot = header.slotsFor(alias).head

      val oldColumnName = columnName(oldSlot)
      val newColumnName = columnName(newSlot)

      val newData = if (records.data.columns.contains(oldColumnName)) {
        records.data.withColumnRenamed(oldColumnName, newColumnName)
      } else {
        throw IllegalArgumentException(s"a column with name $oldColumnName")
      }

      CAPSRecords.create(header, newData)(records.caps)
    }
  }
}

final case class Project(in: PhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val newData = asSparkSQLExpr(header, expr, records.data) match {
        case None => throw NotImplementedException(s"Projecting $expr")

        case Some(sparkSqlExpr) =>
          val headerNames = header.slotsFor(expr).map(columnName)
          val dataNames = records.data.columns.toSeq

          // TODO: Can optimise for var AS var2 case -- avoid duplicating data
          headerNames.diff(dataNames) match {
            case Seq(one) =>
              // align the name of the column to what the header expects
              val newCol = sparkSqlExpr.as(one)
              val columnsToSelect = records.data.columns
                .map(records.data.col) :+ newCol

              records.data.select(columnsToSelect: _*)
            case _ => throw IllegalStateException(s"Got multiple slots for expression $expr")
          }
      }

      CAPSRecords.create(header, newData)(records.caps)
    }
  }
}

final case class Filter(in: PhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val filteredRows =
        asSparkSQLExpr(records.header, expr, records.data) match {
          case Some(sqlExpr) =>
            records.data.where(sqlExpr)
          case None =>
            val predicate = cypherFilter(header, expr)
            records.data.filter(predicate)
        }

      val selectedColumns = header.slots.map { c =>
        val name = columnName(c)
        filteredRows.col(name)
      }

      val newData = filteredRows.select(selectedColumns: _*)

      CAPSRecords.create(header, newData)(records.caps)
    }
  }
}

final case class ProjectExternalGraph(in: PhysicalOperator, name: String, uri: URI) extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult =
    prev.withGraph(name -> resolve(uri))

}

final case class ProjectPatternGraph(
    in: PhysicalOperator,
    toCreate: Set[ConstructedEntity],
    name: String,
    schema: Schema)
    extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    val input = prev.records

    val baseTable =
      if (toCreate.isEmpty) input
      else createEntities(toCreate, input)

    val patternGraph = CAPSGraph.create(baseTable, schema)(input.caps)
    prev.withGraph(name -> patternGraph)
  }

  private def createEntities(toCreate: Set[ConstructedEntity], records: CAPSRecords): CAPSRecords = {
    val nodes = toCreate.collect { case c: ConstructedNode        => c }
    val rels = toCreate.collect { case r: ConstructedRelationship => r }

    val nodesToCreate = nodes.flatMap(constructNode(_, records))
    val recordsWithNodes = addEntitiesToRecords(nodesToCreate, records)

    val relsToCreate = rels.flatMap(constructRel(_, recordsWithNodes))
    addEntitiesToRecords(relsToCreate, recordsWithNodes)
  }

  private def addEntitiesToRecords(columnsToAdd: Set[(SlotContent, Column)], records: CAPSRecords): CAPSRecords = {
    val newData = columnsToAdd.foldLeft(records.data) {
      case (acc, (expr, col)) =>
        acc.withColumn(columnName(expr), col)
    }

    // TODO: Move header construction to FlatPlanner
    val newHeader = records.header
      .update(
        addContents(columnsToAdd.map(_._1).toSeq)
      )
      ._1

    CAPSRecords.create(newHeader, newData)(records.caps)
  }

  private def constructNode(node: ConstructedNode, records: CAPSRecords): (Set[(SlotContent, Column)]) = {
    val col = org.apache.spark.sql.functions.lit(true)
    val labelTuples: Set[(SlotContent, Column)] = node.labels.map { label =>
      ProjectedExpr(HasLabel(node.v, label)(CTBoolean)) -> col
    }

    labelTuples + (OpaqueField(node.v) -> generateId)
  }

  private def generateId: Column = {
    // id needs to be generated
    // Limits the system to 500 mn partitions
    // The first half of the id space is protected
    // TODO: guarantee that all imported entities have ids in the protected range
    val relIdOffset = 500L << 33
    val firstIdCol = functions.lit(relIdOffset)
    monotonically_increasing_id() + firstIdCol
  }

  private def constructRel(toConstruct: ConstructedRelationship, records: CAPSRecords): (Set[(SlotContent, Column)]) = {
    val ConstructedRelationship(rel, source, target, typ) = toConstruct
    val header = records.header
    val inData = records.data

    // source and target are present: just copy
    val sourceTuple = {
      val slot = header.slotFor(source)
      val col = inData.col(columnName(slot))
      ProjectedExpr(StartNode(rel)(CTInteger)) -> col
    }
    val targetTuple = {
      val slot = header.slotFor(target)
      val col = inData.col(columnName(slot))
      ProjectedExpr(EndNode(rel)(CTInteger)) -> col
    }

    // id needs to be generated
    val relTuple = OpaqueField(rel) -> generateId

    // type is an input
    val typeTuple = {
      val col = org.apache.spark.sql.functions.lit(typ)
      ProjectedExpr(Type(rel)(CTString)) -> col
    }

    Set(sourceTuple, targetTuple, relTuple, typeTuple)
  }
}

final case class RemoveAliases(
    dependentFields: Set[(ProjectedField, ProjectedExpr)],
    in: PhysicalOperator,
    header: RecordHeader)
    extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val renamed = dependentFields.foldLeft(records.data) {
        case (df, (v, expr)) =>
          df.withColumnRenamed(SparkColumnName.of(v), SparkColumnName.of(expr))
      }

      CAPSRecords.create(header, renamed)(records.caps)
    }
  }
}

final case class SelectFields(in: PhysicalOperator, fields: IndexedSeq[Var], header: Option[RecordHeader])
    extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val fieldIndices = fields.zipWithIndex.toMap

      val _header = header.getOrElse(records.header.select(fields.toSet))

      val groupedSlots = _header.slots.sortBy {
        _.content match {
          case content: FieldSlotContent =>
            fieldIndices.getOrElse(content.field, Int.MaxValue)
          case content @ ProjectedExpr(expr) =>
            val deps = expr.dependencies
            deps.headOption
              .filter(_ => deps.size == 1)
              .flatMap(fieldIndices.get)
              .getOrElse(Int.MaxValue)
        }
      }

      val data = records.data
      val columns = groupedSlots.map { s =>
        data.col(columnName(s))
      }
      val newData = records.data.select(columns: _*)

      CAPSRecords.create(_header, newData)(records.caps)
    }
  }
}

final case class SelectGraphs(in: PhysicalOperator, graphs: Set[String]) extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult =
    prev.selectGraphs(graphs)

}

final case class Distinct(in: PhysicalOperator, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val data = records.data
      val columnNames = header.slots.map(slot => data.col(columnName(slot)))
      val relevantColumns = data.select(columnNames: _*)
      val distinctRows = relevantColumns.dropDuplicates(header.fields.toSeq)
      CAPSRecords.create(header, distinctRows)(records.caps)
    }
  }
}

final case class SimpleDistinct(in: PhysicalOperator) extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      CAPSRecords.create(prev.records.header, records.data.distinct())(records.caps)
    }
  }
}

final case class Aggregate(
    in: PhysicalOperator,
    aggregations: Set[(Var, Aggregator)],
    group: Set[Var],
    header: RecordHeader)
    extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val inData = records.data

      def withInnerExpr(expr: Expr)(f: Column => Column) = {
        asSparkSQLExpr(records.header, expr, inData) match {
          case None         => throw NotImplementedException(s"Projecting $expr")
          case Some(column) => f(column)
        }
      }

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
          val columnName = SparkColumnName.from(Some(to.name))
          inner match {
            case Avg(expr) =>
              withInnerExpr(expr)(
                functions
                  .avg(_)
                  .cast(toSparkType(to.cypherType))
                  .as(columnName))

            case CountStar(_) =>
              functions.count(functions.lit(0)).as(columnName)

            // TODO: Consider not implicitly projecting the inner expr here, but rewriting it into a variable in logical planning or IR construction
            case Count(expr) =>
              withInnerExpr(expr)(functions.count(_).as(columnName))

            case Max(expr) =>
              withInnerExpr(expr)(functions.max(_).as(columnName))

            case Min(expr) =>
              withInnerExpr(expr)(functions.min(_).as(columnName))

            case Sum(expr) =>
              withInnerExpr(expr)(functions.sum(_).as(columnName))

            case Collect(expr) =>
              withInnerExpr(expr)(functions.collect_list(_).as(columnName))

            case x =>
              throw NotImplementedException(s"Aggregation function $x")
          }
      }

      val aggregated = data.fold(
        _.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*),
        _.agg(sparkAggFunctions.head, sparkAggFunctions.tail.toSeq: _*)
      )

      CAPSRecords.create(header, aggregated)(records.caps)
    }
  }
}

final case class OrderBy(in: PhysicalOperator, sortItems: Seq[SortItem[Expr]], header: RecordHeader)
    extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    val getColumnName = (expr: Var) => columnName(prev.records.header.slotFor(expr))

    val sortExpression = sortItems.map {
      case Asc(expr: Var)  => asc(getColumnName(expr))
      case Desc(expr: Var) => desc(getColumnName(expr))
      case other           => throw IllegalArgumentException("ASC or DESC", other)
    }

    prev.mapRecordsWithDetails { records =>
      val sortedData = records.toDF().sort(sortExpression: _*)
      CAPSRecords.create(header, sortedData)(records.caps)
    }
  }
}

final case class Skip(in: PhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    val skip: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(v) => v
          case other            => throw IllegalArgumentException("a CypherInteger", other)
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
      CAPSRecords.create(header, newDf)(records.caps)
    }
  }
}

final case class Limit(in: PhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    val limit = expr match {
      case IntegerLit(v) => v
      case other         => throw IllegalArgumentException("an integer literal", other)
    }

    prev.mapRecordsWithDetails { records =>
      CAPSRecords.create(header, records.toDF().limit(limit.toInt))(records.caps)
    }
  }
}

// Initialises the table in preparation for variable length expand.
final case class InitVarExpand(in: PhysicalOperator, source: Var, edgeList: Var, target: Var, header: RecordHeader)
    extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult = {
    val sourceSlot = header.slotFor(source)
    val edgeListSlot = header.slotFor(edgeList)
    val targetSlot = header.slotFor(target)

    assertIsNode(targetSlot)

    prev.mapRecordsWithDetails { records =>
      val inputData = records.data
      val keep = inputData.columns.map(inputData.col)

      val edgeListColName = columnName(edgeListSlot)
      val edgeListColumn = udf(udfUtils.initArray _, ArrayType(LongType))()
      val withEmptyList = inputData.withColumn(edgeListColName, edgeListColumn)

      val cols = keep ++ Seq(
        withEmptyList.col(edgeListColName),
        inputData.col(columnName(sourceSlot)).as(columnName(targetSlot)))

      val initializedData = withEmptyList.select(cols: _*)

      CAPSRecords.create(header, initializedData)(records.caps)
    }
  }
}

final case class EmptyRecords(in: PhysicalOperator, header: RecordHeader)(implicit caps: CAPSSession)
    extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult =
    prev.mapRecordsWithDetails(_ => CAPSRecords.empty(header))

}

final case class SetSourceGraph(in: PhysicalOperator, graph: LogicalExternalGraph) extends UnaryPhysicalOperator {

  override def executeUnary(prev: PhysicalResult)(implicit context: RuntimeContext): PhysicalResult =
    prev.withGraph(graph.name -> resolve(graph.uri))

}
