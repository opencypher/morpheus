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
import org.apache.spark.sql.functions.{asc, desc, monotonically_increasing_id}
import org.apache.spark.sql.types.{StructField, StructType}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.ir.impl.syntax.ExprSyntax._
import org.opencypher.okapi.logical.impl.{ConstructedEntity, _}
import org.opencypher.okapi.relational.impl.syntax.RecordHeaderSyntax._
import org.opencypher.okapi.relational.impl.table.{ColumnName, _}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.impl.convert.CAPSCypherType._
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator._
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords}
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

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

final case class Scan(in: CAPSPhysicalOperator, v: Var, header: RecordHeader)
  extends UnaryPhysicalOperator {

  // TODO: Move to Graph interface?
  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val graph = prev.graph
    val records = v.cypherType match {
      case r: CTRelationship =>
        graph.relationships(v.name, r)
      case n: CTNode =>
        graph.nodes(v.name, n)
      case x =>
        throw IllegalArgumentException("an entity type", x)
    }
    assert(header == records.header)
    CAPSPhysicalResult(records, graph)
  }
}

final case class Unwind(in: CAPSPhysicalOperator, list: Expr, item: Var, header: RecordHeader)
  extends UnaryPhysicalOperator {

  import scala.collection.JavaConverters._

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val itemColumn = ColumnName.of(header.slotFor(item))
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
      val oldSlot = records.header.slotsFor(expr).head

      val newSlot = header.slotsFor(alias).head

      val oldColumnName = ColumnName.of(oldSlot)
      val newColumnName = ColumnName.of(newSlot)

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
      val headerNames = header.slotsFor(expr).map(ColumnName.of)
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

final case class Filter(in: CAPSPhysicalOperator, expr: Expr, header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val filteredRows = records.data.where(expr.asSparkSQLExpr(header, records.data, context))

      // TODO: is this necessary? Filter should just remove rows
      val selectedColumns = header.slots.map { c =>
        val name = ColumnName.of(c)
        filteredRows.col(name)
      }

      val newData = filteredRows.select(selectedColumns: _*)

      CAPSRecords.verifyAndCreate(header, newData)(records.caps)
    }
  }
}

final case class ConstructGraph(
  in: CAPSPhysicalOperator,
  clonedItems: Set[ConstructedEntity],
  newItems: Set[ConstructedEntity],
  setItems: List[SetPropertyItem[Expr]],
  initialSchema: CAPSSchema)
  extends UnaryPhysicalOperator {

  override def header: RecordHeader = RecordHeader.empty

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    implicit val session = prev.records.caps
    val inputTable = prev.records

    val newEntityTag = if (initialSchema.tags.isEmpty) 0 else initialSchema.tags.max + 1
    val entityTable = createEntities(newItems, inputTable, newEntityTag)

    val tableWithConstructedProperties = setProperties(setItems, entityTable)

    // Remove input columns and header
    val inputColumns = inputTable.data.columns.toSet

    val (removeHeader, removeColumns) = clonedItems.foldLeft((inputTable.header, inputColumns)) {
      case ((header, columns), nextClone) =>
        val cloneSlots = header.selfWithChildren(nextClone.v)
        val nextHeader = header -- RecordHeader.from(cloneSlots.toList)

        val nextColumns = cloneSlots.map(ColumnName.of).toSet
        nextHeader -> (columns -- nextColumns)
    }

    val withInputsRemoved = CAPSRecords.verifyAndCreate(
      tableWithConstructedProperties.header -- removeHeader,
      tableWithConstructedProperties.data.drop(removeColumns.toSeq: _*))

    val patternGraphSchema = initialSchema.withTags(initialSchema.tags + newEntityTag).asCaps
    val patternGraph = CAPSGraph.create(withInputsRemoved, patternGraphSchema)
    CAPSPhysicalResult(CAPSRecords.unit(), patternGraph)
  }

  private def setProperties(setItems: List[SetPropertyItem[Expr]], constructedTable: CAPSRecords)(implicit context: CAPSRuntimeContext): CAPSRecords = {
    setItems.foldLeft(constructedTable) { case (table, nextSetItem) =>
      constructProperty(nextSetItem.variable, nextSetItem.propertyKey, nextSetItem.setValue, table)
    }
  }

  def constructProperty(variable: Var, propertyKey: String, propertyValue: Expr, constructedTable: CAPSRecords)(implicit context: CAPSRuntimeContext): CAPSRecords = {
    val propertyValueColumn: Column = propertyValue.asSparkSQLExpr(constructedTable.header, constructedTable.data, context)

    val propertyExpression = Property(variable, PropertyKey(propertyKey))(propertyValue.cypherType)
    val propertySlotContent = ProjectedExpr(propertyExpression)
    val newData = constructedTable.data.safeAddColumn(ColumnName.of(propertySlotContent), propertyValueColumn)

    val newHeader = constructedTable.header.update(addContent(propertySlotContent))._1

    CAPSRecords.verifyAndCreate(newHeader, newData)(constructedTable.caps)
  }

  private def createEntities(toCreate: Set[ConstructedEntity], constructedTable: CAPSRecords, newEntityTag: Int): CAPSRecords = {
    val numberOfColumnPartitions = toCreate.size

    // Construct nodes before relationships, as relationships might depend on nodes
    val nodes = toCreate.collect { case c: ConstructedNode => c }
    val rels = toCreate.collect { case r: ConstructedRelationship => r }

    val (_, createdNodes) = nodes.foldLeft(0 -> Set.empty[(SlotContent, Column)]) {
      case ((nextColumnPartitionId, constructedNodes), nextNodeToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedNodes ++ constructNode(newEntityTag, nextColumnPartitionId, numberOfColumnPartitions, nextNodeToConstruct, constructedTable))
    }

    val recordsWithNodes = addEntitiesToRecords(createdNodes, constructedTable)

    val (_, createdRels) = rels.foldLeft(0 -> Set.empty[(SlotContent, Column)]) {
      case ((nextColumnPartitionId, constructedRels), nextRelToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedRels ++ constructRel(newEntityTag, nextColumnPartitionId, numberOfColumnPartitions, nextRelToConstruct, recordsWithNodes))
    }

    addEntitiesToRecords(createdRels, recordsWithNodes)
  }

  private def addEntitiesToRecords(columnsToAdd: Set[(SlotContent, Column)], constructedTable: CAPSRecords): CAPSRecords = {
    val newData = columnsToAdd.foldLeft(constructedTable.data) {
      case (acc, (expr, col)) =>
        acc.safeAddColumn(ColumnName.of(expr), col)
    }

    // TODO: Move header construction to FlatPlanner
    val newHeader = constructedTable.header
      .update(
        addContents(columnsToAdd.map(_._1).toSeq)
      )
      ._1

    CAPSRecords.verifyAndCreate(newHeader, newData)(constructedTable.caps)
  }

  private def constructNode(
    newEntityTag: Int,
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    node: ConstructedNode,
    constructedTable: CAPSRecords
  ): Set[(SlotContent, Column)] = {
    val col = functions.lit(true)
    val labelTuples: Set[(SlotContent, Column)] = node.labels.map { label =>
      ProjectedExpr(HasLabel(node.v, label)(CTBoolean)) -> col
    }

    // TODO: Use to implement COPY OF
    //    val propertyTuples = node.equivalence match {
    //      case Some(TildeModel(origNode)) =>
    //        val header = constructedTable.header
    //        val origSlots = header.propertySlots(origNode).values
    //        val copySlotContents = origSlots.map(_.withOwner(node.v)).map(_.content)
    //        val columns = origSlots.map(ColumnName.of).map(constructedTable.data.col)
    //        copySlotContents.zip(columns).toSet
    //
    //      case Some(AtModel(_)) => throw NotImplementedException("AtModel copies")
    //
    //      case None => Set.empty[(SlotContent, Column)]
    //    }

    val propertyTuples = Set.empty[(SlotContent, Column)]

    labelTuples ++ propertyTuples + (OpaqueField(node.v) -> generateId(columnIdPartition, numberOfColumnPartitions).setTag(newEntityTag))
  }

  /**
    *org.apache.spark.sql.functions$#monotonically_increasing_id()
    *
    * @param columnIdPartition column partition within DF partition
    */
  private def generateId(columnIdPartition: Int, numberOfColumnPartitions: Int): Column = {
    val columnPartitionBits = math.log(numberOfColumnPartitions).floor.toInt + 1
    val totalIdSpaceBits = 33
    val columnIdShift = totalIdSpaceBits - columnPartitionBits

    // id needs to be generated
    // Limits the system to 500 mn partitions
    // The first half of the id space is protected
    val columnPartitionOffset = columnIdPartition << columnIdShift
    monotonically_increasing_id() + functions.lit(columnPartitionOffset)
  }

  private def constructRel(
    newEntityTag: Int,
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    toConstruct: ConstructedRelationship,
    constructedTable: CAPSRecords
  ): Set[(SlotContent, Column)] = {
    val ConstructedRelationship(rel, source, target, typOpt) = toConstruct
    val header = constructedTable.header
    val inData = constructedTable.data

    // source and target are present: just copy
    val sourceTuple = {
      val slot = header.slotFor(source)
      val col = inData.col(ColumnName.of(slot))
      ProjectedExpr(StartNode(rel)(CTInteger)) -> col
    }
    val targetTuple = {
      val slot = header.slotFor(target)
      val col = inData.col(ColumnName.of(slot))
      ProjectedExpr(EndNode(rel)(CTInteger)) -> col
    }

    // id needs to be generated
    val relTuple = OpaqueField(rel) -> generateId(columnIdPartition, numberOfColumnPartitions).setTag(newEntityTag)

    //    val typeTuple = {
    //      typOpt match {
    //        // type is set
    //        case Some(t) =>
    //          val col = functions.lit(t)
    //          ProjectedExpr(Type(rel)(CTString)) -> col
    //        case None =>
    //          // equivalence model is guaranteed to be present: get rel type from original
    //          val origRel = equivalenceOpt.get.v
    //          val header = constructedTable.header
    //          val origTypeSlot = header.typeSlot(origRel)
    //          val copyTypeSlotContents = origTypeSlot.withOwner(rel).content
    //          val col = constructedTable.data.col(ColumnName.of(origTypeSlot))
    //          (copyTypeSlotContents, col)
    //      }
    //    }
    val typeTuple = typOpt match {
      case Some(t) =>
        val col = functions.lit(t)
        ProjectedExpr(Type(rel)(CTString)) -> col

      case None => throw UnsupportedOperationException("Constructing a relationship without a type")
    }

    // TODO: Use to implement COPY OF
//    val propertyTuples = equivalenceOpt match {
//      case Some(TildeModel(origRel)) =>
//        val header = constructedTable.header
//        val origSlots = header.propertySlots(origRel).values
//        val copySlotContents = origSlots.map(_.withOwner(rel)).map(_.content)
//        val columns = origSlots.map(ColumnName.of).map(constructedTable.data.col)
//        copySlotContents.zip(columns).toSet
//      case Some(AtModel(_)) => throw NotImplementedException("AtModel copies")
//
//      case None => Set.empty[(SlotContent, Column)]
//    }
    val propertyTuples = Set.empty[(SlotContent, Column)]

    Set(sourceTuple, targetTuple, relTuple, typeTuple) ++ propertyTuples
  }
}

final case class RemoveAliases(
  in: CAPSPhysicalOperator,
  dependentFields: Set[(ProjectedField, ProjectedExpr)],
  header: RecordHeader) extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val renamed = dependentFields.foldLeft(records.data) {
        case (df, (v, expr)) =>
          df.safeRenameColumn(ColumnName.of(v), ColumnName.of(expr))
      }

      CAPSRecords.verifyAndCreate(header, renamed)(records.caps)
    }
  }
}

final case class ReturnGraph(in: CAPSPhysicalOperator) extends UnaryPhysicalOperator {
  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    CAPSPhysicalResult(CAPSRecords.empty(header)(prev.records.caps), prev.graph)
  }

  override def header: RecordHeader = RecordHeader.empty
}

final case class SelectFields(in: CAPSPhysicalOperator, fields: List[Var], header: RecordHeader)
  extends UnaryPhysicalOperator {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val fieldIndices = fields.zipWithIndex.toMap

      val groupedSlots = header.slots.sortBy {
        _.content match {
          case content: FieldSlotContent =>
            fieldIndices.getOrElse(content.field, Int.MaxValue)
          case content@ProjectedExpr(expr) =>
            val deps = expr.dependencies
            deps.headOption
              .filter(_ => deps.size == 1)
              .flatMap(fieldIndices.get)
              .getOrElse(Int.MaxValue)
        }
      }

      val data = records.data
      val columns = groupedSlots.map { s =>
        data.col(ColumnName.of(s))
      }
      val newData = records.data.select(columns: _*)

      CAPSRecords.verifyAndCreate(header, newData)(records.caps)
    }
  }
}

final case class Distinct(in: CAPSPhysicalOperator, fields: Set[Var])
  extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    prev.mapRecordsWithDetails { records =>
      val data = records.data
      val relevantColumns = fields.map(header.slotFor).map(ColumnName.of)
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
            val withChildren = records.header.selfWithChildren(expr).map(_.content.key)
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

              list.as(columnName)
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
    val getColumnName = (expr: Var) => ColumnName.of(prev.records.header.slotFor(expr))

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
    val sourceSlot = header.slotFor(source)
    val edgeListSlot = header.slotFor(edgeList)
    val targetSlot = header.slotFor(target)

    assertIsNode(targetSlot)

    prev.mapRecordsWithDetails { records =>
      val inputData = records.data
      val keep = inputData.columns.map(inputData.col)

      val edgeListColName = columnName(edgeListSlot)
      val edgeListColumn = functions.typedLit(Array[Long]())
      val withEmptyList = inputData.safeAddColumn(edgeListColName, edgeListColumn)

      val cols = keep ++ Seq(
        withEmptyList.col(edgeListColName),
        inputData.col(columnName(sourceSlot)).as(columnName(targetSlot)))

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

final case class UseGraph(in: CAPSPhysicalOperator, graph: LogicalCatalogGraph) extends UnaryPhysicalOperator with InheritedHeader {

  override def executeUnary(prev: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    CAPSPhysicalResult(prev.records, resolve(graph.qualifiedGraphName))
  }

}
