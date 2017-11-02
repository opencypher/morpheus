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

import java.net.URI

import org.apache.spark.sql.functions.{asc, desc, monotonically_increasing_id, udf}
import org.apache.spark.sql.types.{ArrayType, BooleanType, LongType}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, functions}
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSSession}
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CypherInteger
import org.opencypher.caps.demo.Configuration.DebugPhysicalResult
import org.opencypher.caps.impl.flat.FreshVariableNamer
import org.opencypher.caps.impl.logical._
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.SparkSQLExprMapper.asSparkSQLExpr
import org.opencypher.caps.impl.spark.convert.toSparkType
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.spark.physical.PhysicalOperator._
import org.opencypher.caps.impl.syntax.expr._
import org.opencypher.caps.impl.syntax.header.{addContents, _}
import org.opencypher.caps.ir.api.block.{Asc, Desc, SortItem}

import scala.collection.mutable

trait PhysicalOperator {

  // does the actual work
  protected def run(implicit context: RuntimeContext): PhysicalResult

  final def execute(implicit context: RuntimeContext): PhysicalResult = {
    if (DebugPhysicalResult.get()) {
      val r = run
      println(s"${getClass.getSimpleName}($describeParameters)")
      println("output table:")
      r.records.data.show()
      println(r.graphs.keySet.mkString("Output graphs: ", ", ", ""))
      r
    } else {
      run
    }
  }

  protected def resolve(uri: URI)(implicit context: RuntimeContext): CAPSGraph = {
    context.resolve(uri).getOrElse(Raise.graphNotFound(uri))
  }

  protected def prefix(depth: Int): String = ("· " * depth ) + "|-"
  def pretty(depth: Int = 0): String
  protected def describeParameters: String = ""
}

sealed trait PhysicalLeafOperator extends PhysicalOperator {
  override def pretty(depth: Int): String =
    s"${prefix(depth)} ${getClass.getSimpleName}"
}

sealed trait StackingPhysicalOperator extends PhysicalOperator {
  def in: PhysicalOperator

  protected def prev(implicit context: RuntimeContext): PhysicalResult = in.execute

  override def pretty(depth: Int): String = {
    s"""${prefix(depth)} ${getClass.getSimpleName}($describeParameters)
       #${in.pretty(depth + 1)}""".stripMargin('#')
  }
}

sealed trait BinaryPhysicalOperator extends PhysicalOperator {
  def lhs: PhysicalOperator
  def rhs: PhysicalOperator

  protected def left(implicit context: RuntimeContext): PhysicalResult = lhs.execute
  protected def right(implicit context: RuntimeContext): PhysicalResult = rhs.execute

  protected def constructResult(binaryOp: (CAPSRecords, CAPSRecords) => CAPSRecords)(implicit context: RuntimeContext): PhysicalResult = {
    val resultRecords = binaryOp(left.records, right.records)
    val graphs = left.graphs ++ right.graphs
    PhysicalResult(resultRecords, graphs)
  }

  override def pretty(depth: Int): String = {
    s"""${prefix(depth)} ${getClass.getSimpleName}($describeParameters)
       #${lhs.pretty(depth + 1)}
       #${rhs.pretty(depth + 1)}""".stripMargin('#')
  }
}

sealed trait TernaryPhysicalOperator extends PhysicalOperator {
  def first:  PhysicalOperator
  def second: PhysicalOperator
  def third:  PhysicalOperator

  protected def constructResult(ternaryOp: (CAPSRecords, CAPSRecords, CAPSRecords) => CAPSRecords)(implicit context: RuntimeContext): PhysicalResult = {
    val firstR = first.execute
    val secondR = second.execute
    val thirdR = third.execute
    val resultRecords = ternaryOp(firstR.records, secondR.records, thirdR.records)
    val graphs = firstR.graphs ++ secondR.graphs ++ thirdR.graphs
    PhysicalResult(resultRecords, graphs)
  }

  override def pretty(depth: Int): String = {
    s"""${prefix(depth)} ${getClass.getSimpleName}($describeParameters)
       #${first.pretty(depth + 1)}
       #${second.pretty(depth + 1)}
       #${third.pretty(depth + 1)}""".stripMargin('#')
  }
}

final case class Scan(inGraph: LogicalGraph, v: Var, in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    val graphs = prev.graphs
    val graph = graphs(inGraph.name)

    val records = scan(graph, v)

    prev.mapRecordsWithDetails(_ => records)
  }

  // TODO: Move to Graph interface?
  private def scan(graph: CAPSGraph, v: Var): CAPSRecords = {
    v.cypherType match {
      case r: CTRelationship =>
        graph.relationships(v.name, r)
      case n: CTNode =>
        graph.nodes(v.name, n)
      case x =>
        Raise.invalidArgument("an entity type", x.toString)
    }
  }
}

final case class Alias(expr: Expr, alias: Var, header: RecordHeader, in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val oldSlot = records.header.slotsFor(expr).head

      val newSlot = header.slotsFor(alias).head

      val oldColumnName = columnName(oldSlot)
      val newColumnName = columnName(newSlot)

      val newData = if (records.data.columns.contains(oldColumnName)) {
        records.data.withColumnRenamed(oldColumnName, newColumnName)
      } else {
        Raise.columnNotFound(oldColumnName)
      }

      CAPSRecords.create(header, newData)(records.caps)
    }
  }
}

final case class Project(expr: Expr, header: RecordHeader, in: PhysicalOperator) extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val newData = asSparkSQLExpr(header, expr, records.data) match {
        case None => Raise.notYetImplemented(s"projecting $expr")

        case Some(sparkSqlExpr) =>
          val headerNames = header.slotsFor(expr).map(columnName)
          val dataNames = records.data.columns.toSeq

          // TODO: Can optimise for var AS var2 case -- avoid duplicating data
          headerNames.diff(dataNames) match {
            case Seq(one) =>
              // align the name of the column to what the header expects
              val newCol = sparkSqlExpr.as(one)
              val columnsToSelect = records.data.columns.map(records.data.col) :+ newCol

              records.data.select(columnsToSelect: _*)
            case _ => Raise.multipleSlotsForExpression()
          }
      }

      CAPSRecords.create(header, newData)(records.caps)
    }
  }
}

final case class Filter(expr: Expr, header: RecordHeader, in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val filteredRows = asSparkSQLExpr(records.header, expr, records.data) match {
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

final case class ProjectExternalGraph(name: String, uri: URI, in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    prev.withGraph(name -> resolve(uri))
  }
}

final case class ProjectPatternGraph(toCreate: Set[ConstructedEntity], name: String, schema: Schema, in: PhysicalOperator) extends StackingPhysicalOperator {

  override protected def describeParameters: String = s"${toCreate.mkString("creates = {", ", ", "}")}"

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    val input = prev.records

    val baseTable =
      if (toCreate.isEmpty) input
      else createEntities(toCreate, input)

    val patternGraph = CAPSGraph.create(baseTable, schema)(input.caps)
    prev.withGraph(name -> patternGraph)
  }

  private def createEntities(toCreate: Set[ConstructedEntity], records: CAPSRecords): CAPSRecords = {
    val nodes = toCreate.collect { case c: ConstructedNode => c }
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
    val newHeader = records.header.update(
      addContents(columnsToAdd.map(_._1).toSeq)
    )._1

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
      ProjectedExpr(OfType(rel)(CTString)) -> col
    }

    Set(sourceTuple, targetTuple, relTuple, typeTuple)
  }
}

final case class SelectFields(fields: IndexedSeq[Var], header: Option[RecordHeader], in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override protected def describeParameters: String = s"${fields.mkString("fields = {", ", ", "}")}"

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val fieldIndices = fields.zipWithIndex.toMap

      val _header = header.getOrElse(records.header.select(fields.toSet))

      val groupedSlots = _header.slots.sortBy {
        _.content match {
          case content: FieldSlotContent =>
            fieldIndices.getOrElse(content.field, Int.MaxValue)
          case content@ProjectedExpr(expr) =>
            val deps = expr.dependencies
            deps.headOption.filter(_ => deps.size == 1).flatMap(fieldIndices.get).getOrElse(Int.MaxValue)
        }
      }

      val data = records.data
      val columns = groupedSlots.map { s => data.col(columnName(s)) }
      val newData = records.data.select(columns: _*)

      CAPSRecords.create(_header, newData)(records.caps)
    }
  }
}

final case class SelectGraphs(graphs: Set[String], in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    prev.selectGraphs(graphs)
  }
}

final case class Distinct(header: RecordHeader, in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val data = records.data
      val columnNames = header.slots.map(slot => data.col(columnName(slot)))
      val relevantColumns = data.select(columnNames: _*)
      val distinctRows = relevantColumns.distinct()
      CAPSRecords.create(header, distinctRows)(records.caps)
    }
  }
}

final case class SimpleDistinct(in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      CAPSRecords.create(prev.records.header, records.data.distinct())(records.caps)
    }
  }
}

final case class Aggregate(aggregations: Set[(Var, Aggregator)], group: Set[Var], header: RecordHeader, in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override protected def describeParameters: String = s"${aggregations.map {
    case (v, agg) => s"$agg AS $v"
  } .mkString("agg = {", ", ", "}")}"

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val inData = records.data

      def withInnerExpr(expr: Expr)(f: Column => Column) = {
        asSparkSQLExpr(records.header, expr, inData) match {
          case None => Raise.notYetImplemented(s"projecting $expr")
          case Some(column) => f(column)
        }
      }

      val data: Either[RelationalGroupedDataset, DataFrame] = if (group.nonEmpty) {
        val columns = group.map { expr =>
          withInnerExpr(expr)(identity)
        }
        Left(inData.groupBy(columns.toSeq: _*))
      } else Right(inData)

      val sparkAggFunctions = aggregations.map {
        case (to, inner) =>
          val columnName = SparkColumnName.from(Some(to.name))
          inner match {
            case Avg(expr) =>
              withInnerExpr(expr)(functions.avg(_).cast(toSparkType(to.cypherType)).as(columnName))

            case CountStar() =>
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

            case x =>
              Raise.notYetImplemented(s"Aggregator $x")
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

final case class ValueJoin(predicates: Set[org.opencypher.caps.api.expr.Equals], header: RecordHeader, lhs: PhysicalOperator, rhs: PhysicalOperator)
  extends BinaryPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    val leftHeader = left.records.header
    val rightHeader = right.records.header
    val slots = predicates.map { p =>
      leftHeader.slotsFor(p.lhs).head -> rightHeader.slotsFor(p.rhs).head
    }.toSeq
    constructResult(joinRecords(header, slots))
  }
}

final case class Optional(lhsHeader: RecordHeader, rhsHeader: RecordHeader, lhs: PhysicalOperator, rhs: PhysicalOperator)
  extends BinaryPhysicalOperator {

  private def optional(leftRecords: CAPSRecords, rightRecords: CAPSRecords): CAPSRecords = {
    val lhsData = leftRecords.toDF()
    val rhsData = rightRecords.toDF()
    val commonFields = rhsHeader.fields.intersect(lhsHeader.fields)

    // Remove all common columns from the right hand side, except the join columns
    val columnsToRemove = commonFields
      .flatMap(rhsHeader.childSlots)
      .map(_.content)
      .map(columnName).toSeq

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
      }).toSeq

    val reducedRhsData = joinColumnMapping
      .foldLeft(rhsData)((acc, col) => acc.withColumnRenamed(col._2, col._3))
      .drop(columnsToRemove: _*)

    val joinCols = joinColumnMapping.map(t => t._1 -> reducedRhsData.col(t._3))

    joinDFs(lhsData, reducedRhsData, rhsHeader, joinCols)("leftouter", deduplicate = true)(leftRecords.caps)
  }

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    constructResult(optional)
  }
}

final case class OrderBy(sortItems: Seq[SortItem[Expr]], header: RecordHeader, in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    val getColumnName = (expr: Var) => columnName(prev.records.header.slotFor(expr))

    val sortExpression = sortItems.map {
      case Asc(expr: Var) => asc(getColumnName(expr))
      case Desc(expr: Var) => desc(getColumnName(expr))
      case _ => Raise.impossible()
    }

    prev.mapRecordsWithDetails { records =>
      val sortedData = records.toDF().sort(sortExpression: _*)
      CAPSRecords.create(header, sortedData)(records.caps)
    }
  }
}

final case class Skip(expr: Expr, header: RecordHeader, in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    val skip: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(v) => v
          case _ => Raise.impossible()
        }
      case _ => Raise.impossible()
    }

    // TODO: Replace with data frame based implementation ASAP
    prev.mapRecordsWithDetails { records =>
      val newDf = records.caps.sparkSession.createDataFrame(
        records.toDF().rdd.zipWithIndex().filter((pair) => pair._2 >= skip).map(_._1),
        records.toDF().schema
      )
      CAPSRecords.create(header, newDf)(records.caps)
    }
  }
}

final case class Limit(expr: Expr, header: RecordHeader, in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    val limit = expr match {
      case IntegerLit(v) => v
      case _ => Raise.impossible()
    }

    prev.mapRecordsWithDetails { records =>
      CAPSRecords.create(header, records.toDF().limit(limit.toInt))(records.caps)
    }
  }
}

final case class CartesianProduct(header: RecordHeader, lhs: PhysicalOperator, rhs: PhysicalOperator)
  extends BinaryPhysicalOperator {

  private def cartesian(lhsRecords: CAPSRecords, rhsRecords: CAPSRecords): CAPSRecords = {
    val data = lhsRecords.data
    val otherData = rhsRecords.data
    val newData = data.crossJoin(otherData)
    CAPSRecords.create(header, newData)(lhsRecords.caps)
  }

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    constructResult(cartesian)
  }
}

// This maps a Cypher pattern such as (s)-[r]->(t), where s is solved by first, r is solved by second and t is solved by third
final case class ExpandSource(source: Var, rel: Var, target: Var, header: RecordHeader, first: PhysicalOperator, second: PhysicalOperator, third: PhysicalOperator)
  extends TernaryPhysicalOperator {

  private def expand(firstRecords: CAPSRecords, secondRecords: CAPSRecords, thirdRecords: CAPSRecords): CAPSRecords = {
    val sourceSlot = firstRecords.header.slotFor(source)
    val sourceSlotInRel = secondRecords.header.sourceNodeSlot(rel)
    assertIsNode(sourceSlot)
    assertIsNode(sourceSlotInRel)

    val sourceToRelHeader = firstRecords.header ++ secondRecords.header
    val sourceAndRel = joinRecords(sourceToRelHeader, Seq(sourceSlot -> sourceSlotInRel))(firstRecords, secondRecords)

    val targetSlot = thirdRecords.header.slotFor(target)
    val targetSlotInRel = sourceAndRel.header.targetNodeSlot(rel)
    assertIsNode(targetSlot)
    assertIsNode(targetSlotInRel)

    joinRecords(header, Seq(targetSlotInRel -> targetSlot))(sourceAndRel, thirdRecords)
  }

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    constructResult(expand)
  }
}

// This maps a Cypher pattern such as (s)-[r]->(t), where s and t are both solved by lhs, and r is solved by rhs
final case class ExpandInto(
  source: Var, rel: Var, target: Var,
  header: RecordHeader, lhs: PhysicalOperator, rhs: PhysicalOperator
)
  extends BinaryPhysicalOperator {

  private def expandInto(lhsRecords: CAPSRecords, rhsRecords: CAPSRecords): CAPSRecords = {
    val sourceSlot = lhsRecords.header.slotFor(source)
    val targetSlot = lhsRecords.header.slotFor(target)
    val relSourceSlot = rhsRecords.header.sourceNodeSlot(rel)
    val relTargetSlot = rhsRecords.header.targetNodeSlot(rel)

    assertIsNode(sourceSlot)
    assertIsNode(targetSlot)
    assertIsNode(relSourceSlot)
    assertIsNode(relTargetSlot)

    joinRecords(header, Seq(sourceSlot -> relSourceSlot, targetSlot -> relTargetSlot))(lhsRecords, rhsRecords)
  }

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    constructResult(expandInto)
  }
}

// Initialises the table in preparation for variable length expand.
final case class InitVarExpand(source: Var, edgeList: Var, target: Var, header: RecordHeader, in: PhysicalOperator)
  extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
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

// Expands a pattern like (s)-[r*n..m]->(t) where s is solved by first, r is solved by second and t is solved by third
// this performs m joins with second to step all steps, then drops n of these steps
// edgeList is what is bound to r; a list of relationships (currently just the ids)
final case class BoundedVarExpand(
  rel: Var, edgeList: Var, target: Var, initialEndNode: Var,
  lower: Int, upper: Int, header: RecordHeader,
  first: PhysicalOperator, second: PhysicalOperator, third: PhysicalOperator,
  isExpandInto: Boolean)
  extends TernaryPhysicalOperator {

  private def varExpand(firstRecords: CAPSRecords, secondRecords: CAPSRecords, thirdRecords: CAPSRecords): CAPSRecords = {
    val expanded = expand(firstRecords, secondRecords)

    finalize(expanded, thirdRecords)
  }

  private def iterate(lhs: DataFrame, rels: DataFrame)
    (endNode: RecordSlot, rel: Var, relStartNode: RecordSlot,
      listTempColName: String, edgeListColName: String, keep: Array[String]): DataFrame = {

    val relIdColumn = rels.col(columnName(OpaqueField(rel)))
    val startColumn = rels.col(columnName(relStartNode))
    val expandColumnName = columnName(endNode)
    val expandColumn = lhs.col(expandColumnName)

    val joined = lhs.join(rels, expandColumn === startColumn, "inner")

    val appendUdf = udf(udfUtils.arrayAppend _, ArrayType(LongType))
    val extendedArray = appendUdf(lhs.col(edgeListColName), relIdColumn)
    val withExtendedArray = joined.withColumn(listTempColName, extendedArray)
    val arrayContains = udf(udfUtils.contains _, BooleanType)(withExtendedArray.col(edgeListColName), relIdColumn)
    val filtered = withExtendedArray.filter(!arrayContains)

    // TODO: Try and get rid of the Var rel here
    val endNodeIdColNameOfJoinedRel = columnName(ProjectedExpr(EndNode(rel)(CTNode)))

    val columns = keep ++ Seq(listTempColName, endNodeIdColNameOfJoinedRel)
    val withoutRelProperties = filtered.select(columns.head, columns.tail: _*)  // drops joined columns from relationship table

    withoutRelProperties
      .drop(expandColumn)
      .withColumnRenamed(endNodeIdColNameOfJoinedRel, expandColumnName)
      .drop(edgeListColName)
      .withColumnRenamed(listTempColName, edgeListColName)
  }

  private def finalize(expanded: CAPSRecords, targets: CAPSRecords): CAPSRecords = {
    val endNodeSlot = expanded.header.slotFor(initialEndNode)
    val endNodeCol = columnName(endNodeSlot)

    val targetNodeSlot = targets.header.slotFor(target)
    val targetNodeCol = columnName(targetNodeSlot)

    // If the expansion ends in an already solved plan, the final join can be replaced by a filter.
    val result = if (isExpandInto) {
        val data = expanded.toDF()
        CAPSRecords.create(header,
          data.filter(data.col(targetNodeCol) === data.col(endNodeCol)))(expanded.caps)
    } else {
      val joinHeader = expanded.header ++ targets.header

      val lhsSlot = expanded.header.slotFor(initialEndNode)
      val rhsSlot = targets.header.slotFor(target)

      assertIsNode(lhsSlot)
      assertIsNode(rhsSlot)

      joinRecords(joinHeader, Seq(lhsSlot -> rhsSlot))(expanded, targets)
    }

    CAPSRecords.create(header, result.toDF().drop(endNodeCol))(expanded.caps)
  }

  private def expand(firstRecords: CAPSRecords, secondRecords: CAPSRecords): CAPSRecords = {
    val initData = firstRecords.data
    val relsData = secondRecords.data

    val edgeListColName = columnName(firstRecords.header.slotFor(edgeList))

    val steps = new mutable.HashMap[Int, DataFrame]
    steps(0) = initData

    val keep = initData.columns

    val listTempColName = FreshVariableNamer.generateUniqueName(firstRecords.header)

    val startSlot = secondRecords.header.sourceNodeSlot(rel)
    val endNodeSlot = firstRecords.header.slotFor(initialEndNode)
    (1 to upper).foreach { i =>
      // TODO: Check whether we can abort iteration if result has no cardinality (eg count > 0?)
      steps(i) = iterate(steps(i - 1), relsData)(endNodeSlot, rel, startSlot, listTempColName, edgeListColName, keep)
    }

    val union = steps.filterKeys(_ >= lower).values.reduce[DataFrame] {
      case (l, r) => l.union(r)
    }

    CAPSRecords.create(firstRecords.header, union)(firstRecords.caps)
  }

  override def run(implicit context: RuntimeContext): PhysicalResult = constructResult(varExpand)
}

final case class StartFrom(records: CAPSRecords, graph: LogicalExternalGraph) extends PhysicalLeafOperator {
  override def run(implicit context: RuntimeContext): PhysicalResult = {
    PhysicalResult(records, Map(graph.name -> resolve(graph.uri)))
  }
}

final case class Start(graph: LogicalExternalGraph, records: CAPSRecords) extends PhysicalLeafOperator {
  override def run(implicit context: RuntimeContext): PhysicalResult = {
    PhysicalResult(records, Map(graph.name -> resolve(graph.uri)))
  }
}

final case class SetSourceGraph(graph: LogicalExternalGraph, in: PhysicalOperator) extends StackingPhysicalOperator {

  override def run(implicit context: RuntimeContext): PhysicalResult = {
    prev.withGraph(graph.name -> resolve(graph.uri))
  }
}

object PhysicalOperator {
  def columnName(slot: RecordSlot): String = SparkColumnName.of(slot)
  def columnName(content: SlotContent): String = SparkColumnName.of(content)


  def joinRecords(header: RecordHeader, joinSlots: Seq[(RecordSlot, RecordSlot)],
                  joinType: String = "inner", deduplicate: Boolean = false)
                  (lhs: CAPSRecords, rhs: CAPSRecords): CAPSRecords = {

    val lhsData = lhs.toDF()
    val rhsData = rhs.toDF()

    val joinCols = joinSlots.map(pair =>
      lhsData.col(columnName(pair._1)) -> rhsData.col(columnName(pair._2))
    )

    joinDFs(lhsData, rhsData, header, joinCols)(joinType, deduplicate)(lhs.caps)
  }

  def joinDFs(lhsData: DataFrame, rhsData: DataFrame,
              header: RecordHeader, joinCols: Seq[(Column, Column)])
             (joinType: String, deduplicate: Boolean)
             (implicit caps: CAPSSession): CAPSRecords = {

    val joinExpr = joinCols
      .map { case (l, r) => l === r }
      .reduce(_ && _)

    val joinedData = lhsData.join(rhsData, joinExpr, joinType)

    val returnData = if (deduplicate) {
      val colsToDrop = joinCols.map(col => col._2)
      colsToDrop.foldLeft(joinedData)((acc, col) => acc.drop(col))
    } else joinedData

    CAPSRecords.create(header, returnData)
  }

  def assertIsNode(slot: RecordSlot): Unit = {
    slot.content.cypherType match {
      case CTNode(_) =>
      case x => throw new IllegalArgumentException(s"Expected $slot to contain a node, but was $x")
    }
  }

}
