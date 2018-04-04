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

import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{Column, functions}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl.{ConstructedEntity, ConstructedNode, ConstructedRelationship, LogicalPatternGraph}
import org.opencypher.okapi.relational.impl.syntax.RecordHeaderSyntax.{addContent, addContents, _}
import org.opencypher.okapi.relational.impl.table.{ColumnName, _}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSUnionGraph.{apply => _, unapply => _}
import org.apache.spark.sql.functions
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.relational.impl.table.{ColumnName, OpaqueField, RecordHeader, RecordSlot}
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator._
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.spark.impl.util.TagSupport._
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords, CAPSUnionGraph, ColumnNameGenerator}
import org.opencypher.spark.schema.CAPSSchema._

private[spark] abstract class BinaryPhysicalOperator extends CAPSPhysicalOperator {

  def lhs: CAPSPhysicalOperator

  def rhs: CAPSPhysicalOperator

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = executeBinary(lhs.execute, rhs.execute)

  def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult
}

final case class Join(
  lhs: CAPSPhysicalOperator,
  rhs: CAPSPhysicalOperator,
  joinColumns: Seq[(Expr, Expr)],
  header: RecordHeader) extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
    implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {

    val joinSlots = joinColumns.map {
      case (leftExpr, rightExpr) =>
        val leftRecordSlot = header.slotsFor(leftExpr)
          .headOption
          .getOrElse(throw IllegalArgumentException("Expression mapping to a single column", leftExpr))
        val rightRecordSlot = header.slotsFor(rightExpr)
          .headOption
          .getOrElse(throw IllegalArgumentException("Expression mapping to a single column", rightExpr))

        leftRecordSlot -> rightRecordSlot
    }

    val joinedRecords = joinRecords(header, joinSlots)(left.records, right.records)

    CAPSPhysicalResult(joinedRecords, left.workingGraph, left.workingGraphName)
  }
}

/**
  * This operator performs a left outer join between the already matched path and the optional matched pattern and
  * updates the resulting columns.
  *
  * @param lhs    previous match data
  * @param rhs    optional match data
  * @param header result header (lhs header + rhs header)
  */
final case class Optional(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator, header: RecordHeader)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
    implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
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
      .map(ColumnName.of)

    val lhsJoinSlots = joinFields.map(leftHeader.slotFor)
    val rhsJoinSlots = joinFields.map(rightHeader.slotFor)

    // Find the join pairs and introduce an alias for the right hand side
    // This is necessary to be able to deduplicate the join columns later
    val joinColumnMapping = lhsJoinSlots
      .map(lhsSlot => {
        lhsSlot -> rhsJoinSlots.find(_.content == lhsSlot.content).get
      })
      .map(pair => {
        val lhsColName = ColumnName.of(pair._1)
        val rhsColName = ColumnName.of(pair._2)

        (lhsColName, rhsColName, ColumnNameGenerator.generateUniqueName(rightHeader))
      })

    // Rename join columns on the right hand side and drop common non-join columns
    val reducedRhsData = joinColumnMapping
      .foldLeft(rightData)((acc, col) => acc.safeRenameColumn(col._2, col._3))
      .safeDropColumns(columnsToRemove: _*)

    val joinCols = joinColumnMapping.map(t => t._1 -> t._3)
    val joinedRecords =
      joinDFs(left.records.data, reducedRhsData, header, joinCols)("leftouter", deduplicate = true)(left.records.caps)

    CAPSPhysicalResult(joinedRecords, left.workingGraph, left.workingGraphName)
  }
}

/**
  * This operator performs a left outer join between the already matched path and the pattern path. If, for a given,
  * already bound match, there is a non-null partner, we set a target column to true, otherwise false.
  * Only the mandatory match data and the target column are kept in the result.
  *
  * @param lhs         mandatory match data
  * @param rhs         expanded pattern predicate data
  * @param targetField field that will store the subquery value (exists true/false)
  * @param header      result header (lhs header + predicateField)
  */
final case class ExistsSubQuery(
  lhs: CAPSPhysicalOperator,
  rhs: CAPSPhysicalOperator,
  targetField: Var,
  header: RecordHeader)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
    implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val leftData = left.records.toDF()
    val rightData = right.records.toDF()
    val leftHeader = left.records.header
    val rightHeader = right.records.header

    val joinFields = leftHeader.internalHeader.fields.intersect(rightHeader.internalHeader.fields)

    val columnsToRemove = joinFields
      .flatMap(rightHeader.childSlots)
      .map(_.content)
      .map(ColumnName.of)
      .toSeq

    val lhsJoinSlots = joinFields.map(leftHeader.slotFor)
    val rhsJoinSlots = joinFields.map(rightHeader.slotFor)

    // Find the join pairs and introduce an alias for the right hand side
    // This is necessary to be able to deduplicate the join columns later
    val joinColumnMapping = lhsJoinSlots
      .map(lhsSlot => {
        lhsSlot -> rhsJoinSlots.find(_.content == lhsSlot.content).get
      })
      .map(pair => {
        val lhsCol = ColumnName.of(pair._1)
        val rhsColName = ColumnName.of(pair._2)

        (lhsCol, rhsColName, ColumnNameGenerator.generateUniqueName(rightHeader))
      })
      .toSeq

    // Rename join columns on the right hand side and drop common non-join columns
    val reducedRhsData = joinColumnMapping
      .foldLeft(rightData)((acc, col) => acc.safeRenameColumn(col._2, col._3))
      .safeDropColumns(columnsToRemove: _*)

    // Compute distinct rows based on join columns
    val distinctRightData = reducedRhsData.dropDuplicates(joinColumnMapping.map(_._3))

    val joinCols = joinColumnMapping.map(t => t._1 -> t._3)

    val joinedRecords =
      joinDFs(left.records.data, distinctRightData, header, joinCols)("leftouter", deduplicate = true)(left.records.caps)

    val targetFieldColumnName = ColumnName.of(rightHeader.slotFor(targetField))
    val targetFieldColumn = joinedRecords.data.col(targetFieldColumnName)

    // If the targetField column contains no value we replace it with false, otherwise true.
    // After that we drop all right columns and only keep the predicate field.
    // The predicate field is later checked by a filter operator.
    val updatedJoinedRecords = joinedRecords.data
      .safeReplaceColumn(
        targetFieldColumnName,
        functions.when(functions.isnull(targetFieldColumn), false).otherwise(true))

    CAPSPhysicalResult(CAPSRecords.verifyAndCreate(header, updatedJoinedRecords)(left.records.caps), left.workingGraph, left.workingGraphName)
  }
}

/**
  * Computes the union of the two input operators. The two inputs must have identical headers.
  * This operation does not remove duplicates.
  *
  * The output header of this operation is identical to the input headers.
  *
  * @param lhs the first operand
  * @param rhs the second operand
  */
final case class TabularUnionAll(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator)
  extends BinaryPhysicalOperator with InheritedHeader {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val leftData = left.records.data
    // left and right have the same set of columns, but the order must also match
    val rightData = right.records.data.select(leftData.columns.head, leftData.columns.tail: _*)

    val unionedData = leftData.union(rightData)
    val records = CAPSRecords.verifyAndCreate(header, unionedData)(left.records.caps)

    CAPSPhysicalResult(records, left.workingGraph, left.workingGraphName)
  }
}

final case class CartesianProduct(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator, header: RecordHeader)
  extends BinaryPhysicalOperator {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
    implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {

    val data = left.records.data
    val otherData = right.records.data
    val newData = data.crossJoin(otherData)

    val records = CAPSRecords.verifyAndCreate(header, newData)(left.records.caps)
    CAPSPhysicalResult(records, left.workingGraph, left.workingGraphName)
  }
}

/**
  * @param lhs table with aliases and data for new entities
  * @param rhs graph on which we construct the new graph
  * @param construct
  */
final case class ConstructGraph(
  lhs: CAPSPhysicalOperator,
  rhs: CAPSPhysicalOperator,
  construct: LogicalPatternGraph
) extends BinaryPhysicalOperator {

  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }

  override def header: RecordHeader = RecordHeader.empty

  private def pickFreeTag(tagStrategy: Map[QualifiedGraphName, Map[Int, Int]]): Int = {
    val usedTags = tagStrategy.values.flatMap(_.values).toSet
    if (usedTags.isEmpty) 0
    else usedTags.max + 1
  }

  private def identityRetaggings(g: CAPSGraph): (CAPSGraph, Map[Int, Int]) = {
    g -> g.tags.zip(g.tags).toMap
  }

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)
    (implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    implicit val session: CAPSSession = left.records.caps

    val onGraph = right.workingGraph
    val unionTagStrategy: Map[QualifiedGraphName, Map[Int, Int]] = right.tagStrategy

    val LogicalPatternGraph(schema, clonedVarsToInputVars, newEntities, sets, onGraphs, name) = construct

    val matchGraphs: Set[QualifiedGraphName] = clonedVarsToInputVars.values.map(_.cypherType.graph.get).toSet
    val allGraphs = unionTagStrategy.keySet ++ matchGraphs
    val tagsForGraph: Map[QualifiedGraphName, Set[Int]] = allGraphs.map(qgn => qgn -> resolveTags(qgn)).toMap

    val constructTagStrategy = computeRetaggings(tagsForGraph, unionTagStrategy)

    // Apply aliases in CLONE to input table in order to create the base table, on which CONSTRUCT happens
    val aliasClones = clonedVarsToInputVars.filter { case (alias, original) => alias != original }
    val baseTable = left.records.addAliases(aliasClones)

    val retaggedBaseTable = clonedVarsToInputVars.foldLeft(baseTable) { case (df, clone) =>
      df.retagVariable(clone._1, constructTagStrategy(clone._2.cypherType.graph.get))
    }

    // Construct NEW entities
    val (newEntityTags, tableWithConstructedEntities) = {
      if (newEntities.isEmpty) {
        Set.empty[Int] -> retaggedBaseTable
      } else {
        val newEntityTag = pickFreeTag(constructTagStrategy)
        val entityTable = createEntities(newEntities, retaggedBaseTable, newEntityTag)
        val entityTableWithProperties = sets.foldLeft(entityTable) {
          case (df, SetPropertyItem(key, v, expr)) =>
            constructProperty(v, key, expr, df)
        }
        Set(newEntityTag) -> entityTable
      }
    }

    // Remove all vars that were part the original pattern graph DF, except variables that were CLONEd without an alias
    val allInputVars = baseTable.header.internalHeader.fields
    val originalVarsToKeep = clonedVarsToInputVars.keySet -- aliasClones.keySet
    val varsToRemoveFromTable = allInputVars -- originalVarsToKeep
    val patternGraphTable = tableWithConstructedEntities.removeVars(varsToRemoveFromTable)

    val tagsUsed = constructTagStrategy.foldLeft(newEntityTags) {
      case (tags, (qgn, remapping)) =>
        val remappedTags = tagsForGraph(qgn).map(remapping)
        tags ++ remappedTags
    }

    val patternGraph = CAPSGraph.create(patternGraphTable, schema.asCaps, tagsUsed)
    val constructedCombinedWithOn = CAPSUnionGraph(Map(identityRetaggings(onGraph), identityRetaggings(patternGraph)))

    context.patternGraphTags.update(construct.name, constructedCombinedWithOn.tags)

    CAPSPhysicalResult(CAPSRecords.unit(), constructedCombinedWithOn, name, constructTagStrategy)
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
    // Construct nodes before relationships, as relationships might depend on nodes
    val nodes = toCreate.collect {
      case c@ConstructedNode(Var(name), _, _) if !constructedTable.header.fields.contains(name) => c
    }
    val rels = toCreate.collect {
      case r@ConstructedRelationship(Var(name), _, _, _, _) if !constructedTable.header.fields.contains(name) => r
    }

    val (_, createdNodes) = nodes.foldLeft(0 -> Set.empty[(SlotContent, Column)]) {
      case ((nextColumnPartitionId, constructedNodes), nextNodeToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedNodes ++ constructNode(newEntityTag, nextColumnPartitionId, nodes.size, nextNodeToConstruct, constructedTable))
    }

    val recordsWithNodes = addEntitiesToRecords(createdNodes, constructedTable)

    val (_, createdRels) = rels.foldLeft(0 -> Set.empty[(SlotContent, Column)]) {
      case ((nextColumnPartitionId, constructedRels), nextRelToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedRels ++ constructRel(newEntityTag, nextColumnPartitionId, rels.size, nextRelToConstruct, recordsWithNodes))
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

    val propertyTuples = node.equivalence match {
      case Some(TildeModel(origNode)) =>
        val header = constructedTable.header
        val origSlots = header.propertySlots(origNode).values
        val copySlotContents = origSlots.map(_.withOwner(node.v)).map(_.content)
        val columns = origSlots.map(ColumnName.of).map(constructedTable.data.col)
        copySlotContents.zip(columns).toSet

      case Some(AtModel(_)) => throw NotImplementedException("AtModel copies")

      case None => Set.empty[(SlotContent, Column)]
    }

    labelTuples ++ propertyTuples + (OpaqueField(node.v) -> generateId(columnIdPartition, numberOfColumnPartitions).setTag(newEntityTag))
  }

  /**
    *org.apache.spark.sql.functions$#monotonically_increasing_id()
    *
    * @param columnIdPartition column partition within DF partition
    */
  // TODO: improve documentation and add specific tests
  private def generateId(columnIdPartition: Int, numberOfColumnPartitions: Int): Column = {
    val columnPartitionBits = math.log(numberOfColumnPartitions).floor.toInt + 1
    val totalIdSpaceBits = 33
    val columnIdShift = totalIdSpaceBits - columnPartitionBits

    // id needs to be generated
    // Limits the system to 500 mn partitions
    // The first half of the id space is protected
    val columnPartitionOffset = columnIdPartition.toLong << columnIdShift
    monotonically_increasing_id() + functions.lit(columnPartitionOffset)
  }

  private def constructRel(
    newEntityTag: Int,
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    toConstruct: ConstructedRelationship,
    constructedTable: CAPSRecords
  ): Set[(SlotContent, Column)] = {
    val ConstructedRelationship(rel, source, target, typOpt, equivalenceOpt) = toConstruct
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

    val typeTuple = {
      typOpt match {
        // type is set
        case Some(t) =>
          val col = functions.lit(t)
          ProjectedExpr(Type(rel)(CTString)) -> col
        case None =>
          // equivalence model is guaranteed to be present: get rel type from original
          val origRel = equivalenceOpt.get.v
          val header = constructedTable.header
          val origTypeSlot = header.typeSlot(origRel)
          val copyTypeSlotContents = origTypeSlot.withOwner(rel).content
          val col = constructedTable.data.col(ColumnName.of(origTypeSlot))
          (copyTypeSlotContents, col)
      }
    }

    val propertyTuples = equivalenceOpt match {
      case Some(TildeModel(origRel)) =>
        val header = constructedTable.header
        val origSlots = header.propertySlots(origRel).values
        val copySlotContents = origSlots.map(_.withOwner(rel)).map(_.content)
        val columns = origSlots.map(ColumnName.of).map(constructedTable.data.col)
        copySlotContents.zip(columns).toSet
      case Some(AtModel(_)) => throw NotImplementedException("AtModel copies")

      case None => Set.empty[(SlotContent, Column)]
    }

    Set(sourceTuple, targetTuple, relTuple, typeTuple) ++ propertyTuples
  }
}
