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

import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{Column, functions}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTString}
import org.opencypher.okapi.ir.api.expr.{Expr, Var, _}
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.ir.api.{PropertyKey, RelType}
import org.opencypher.okapi.logical.impl.{ConstructedEntity, ConstructedNode, ConstructedRelationship, LogicalPatternGraph}
import org.opencypher.okapi.relational.impl.physical.JoinType
import org.opencypher.okapi.relational.impl.table.RecordHeaderNew
import org.opencypher.spark.api.{CAPSSession, Tags}
import org.opencypher.spark.impl.CAPSUnionGraph.{apply => _, unapply => _}
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.impl.physical.operators.CAPSPhysicalOperator._
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}
import org.opencypher.spark.impl.util.TagSupport._
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords, CAPSUnionGraph}
import org.opencypher.spark.schema.CAPSSchema._

private[spark] abstract class BinaryPhysicalOperator extends CAPSPhysicalOperator {

  def lhs: CAPSPhysicalOperator

  def rhs: CAPSPhysicalOperator

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    executeBinary(lhs.execute, rhs.execute)
  }

  def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)
    (implicit context: CAPSRuntimeContext): CAPSPhysicalResult
}

final case class Join(
  lhs: CAPSPhysicalOperator,
  rhs: CAPSPhysicalOperator,
  joinExprs: Seq[(Expr, Expr)],
  header: RecordHeaderNew,
  joinType: JoinType
) extends BinaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
    implicit context: CAPSRuntimeContext
  ): CAPSPhysicalResult = {

    val joinedRecords = left.records.join(right.records, joinType, joinExprs: _*)
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
  header: RecordHeaderNew
)
  extends BinaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
    implicit context: CAPSRuntimeContext
  ): CAPSPhysicalResult = {
    val leftData = left.records.toDF()
    val rightData = right.records.toDF()
    val leftHeader = left.records.header
    val rightHeader = right.records.header

    val joinFields = leftHeader.vars.intersect(rightHeader.vars)

    val columnsToRemove = joinFields
      .flatMap(v => rightHeader.ownedBy(v) - v)
      .map(rightHeader.column)
      .toSeq

    val lhsJoinColumns = joinFields.map(leftHeader.column)
    val rhsJoinColumns = joinFields.map(rightHeader.column)

    // TODO: Fix
    def generateUniqueColumnName: String = {
      s"tmp${System.nanoTime}"
    }


    // Find the join pairs and introduce an alias for the right hand side
    // This is necessary to be able to deduplicate the join columns later
    val joinColumnMapping = lhsJoinColumns
      .map(lhsColumn => lhsColumn -> rhsJoinColumns.find(_ == lhsColumn).get)
      .map(pair => (pair._1, pair._2, generateUniqueColumnName))
      .toSeq

    // TODO: replace with CAPSRecords#select and CAPSRecords#join
    // Rename join columns on the right hand side and drop common non-join columns
    val reducedRhsData = joinColumnMapping
      .foldLeft(rightData)((acc, col) => acc.safeRenameColumn(col._2, col._3))
      .safeDropColumns(columnsToRemove: _*)

    // Compute distinct rows based on join columns
    val distinctRightData = reducedRhsData.dropDuplicates(joinColumnMapping.map(_._3))

    val joinCols = joinColumnMapping.map(t => t._1 -> t._3)

    val joinedRecords =
      joinDFs(left.records.df, distinctRightData, header, joinCols)("leftouter", deduplicate = true)(left.records.caps)

    val targetFieldColumnName = rightHeader.column(targetField)
    val targetFieldColumn = joinedRecords.df.col(targetFieldColumnName)

    // If the targetField column contains no value we replace it with false, otherwise true.
    // After that we drop all right columns and only keep the predicate field.
    // The predicate field is later checked by a filter operator.
    val updatedJoinedRecords = joinedRecords.df
      .safeReplaceColumn(
        targetFieldColumnName,
        functions.when(functions.isnull(targetFieldColumn), false).otherwise(true))

    CAPSPhysicalResult(CAPSRecords(header, updatedJoinedRecords)(left.records.caps), left.workingGraph, left.workingGraphName)
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
  extends BinaryPhysicalOperator with InheritedHeader with PhysicalOperatorDebugging {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)
    (implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    val leftData = left.records.df
    // left and right have the same set of columns, but the order must also match
    val rightData = right.records.df.select(leftData.columns.head, leftData.columns.tail: _*)

    val unionedData = leftData.union(rightData)
    val records = CAPSRecords(header, unionedData)(left.records.caps)

    CAPSPhysicalResult(records, left.workingGraph, left.workingGraphName)
  }
}

final case class CartesianProduct(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator, header: RecordHeaderNew)
  extends BinaryPhysicalOperator with PhysicalOperatorDebugging {

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)(
    implicit context: CAPSRuntimeContext
  ): CAPSPhysicalResult = {

    val data = left.records.df
    val otherData = right.records.df
    val newData = data.crossJoin(otherData)

    val records = CAPSRecords(header, newData)(left.records.caps)
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
) extends BinaryPhysicalOperator with PhysicalOperatorDebugging {

  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }

  override def header: RecordHeaderNew = RecordHeaderNew.empty

  private def pickFreeTag(tagStrategy: Map[QualifiedGraphName, Map[Int, Int]]): Int = {
    val usedTags = tagStrategy.values.flatMap(_.values).toSet
    Tags.pickFreeTag(usedTags)
  }

  private def identityRetaggings(g: CAPSGraph): (CAPSGraph, Map[Int, Int]) = {
    g -> g.tags.zip(g.tags).toMap
  }

  override def executeBinary(left: CAPSPhysicalResult, right: CAPSPhysicalResult)
    (implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    implicit val session: CAPSSession = left.records.caps

    val onGraph = right.workingGraph
    val unionTagStrategy: Map[QualifiedGraphName, Map[Int, Int]] = right.tagStrategy

    val LogicalPatternGraph(schema, clonedVarsToInputVars, newEntities, sets, _, name) = construct

    val matchGraphs: Set[QualifiedGraphName] = clonedVarsToInputVars.values.map(_.cypherType.graph.get).toSet
    val allGraphs = unionTagStrategy.keySet ++ matchGraphs
    val tagsForGraph: Map[QualifiedGraphName, Set[Int]] = allGraphs.map(qgn => qgn -> resolveTags(qgn)).toMap

    val constructTagStrategy = computeRetaggings(tagsForGraph, unionTagStrategy)

    // Apply aliases in CLONE to input table in order to create the base table, on which CONSTRUCT happens
    val aliasClones = clonedVarsToInputVars.filter { case (alias, original) => alias != original }
    val baseTable = left.records.withAliases(aliasClones.toSeq: _*)

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
        Set(newEntityTag) -> entityTableWithProperties
      }
    }

    // Remove all vars that were part the original pattern graph DF, except variables that were CLONEd without an alias
    val allInputVars = baseTable.header.vars
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


  def constructProperty(variable: Var, propertyKey: String, propertyValue: Expr, constructedTable: CAPSRecords)
    (implicit context: CAPSRuntimeContext): CAPSRecords = {
    val propertyValueColumn: Column = propertyValue.asSparkSQLExpr(constructedTable.header, constructedTable.df, context)

    val propertyExpression = Property(variable, PropertyKey(propertyKey))(propertyValue.cypherType)

    val existingPropertyExpressionsForKey = constructedTable.header.propertiesFor(variable).collect({
      case p@Property(_, PropertyKey(name)) if name == propertyKey => p
    })

    val headerWithExistingRemoved = constructedTable.header -- existingPropertyExpressionsForKey
    val dataWithExistingRemoved = existingPropertyExpressionsForKey.foldLeft(constructedTable.df) {
      case (acc, toRemove) => acc.safeDropColumn(constructedTable.header.column(toRemove))
    }

    val newHeader = headerWithExistingRemoved.withExpr(propertyExpression)
    val newData = dataWithExistingRemoved.safeAddColumn(newHeader.column(propertyExpression), propertyValueColumn)
    CAPSRecords(newHeader, newData)(constructedTable.caps)
  }

  private def createEntities(
    toCreate: Set[ConstructedEntity],
    constructedTable: CAPSRecords,
    newEntityTag: Int): CAPSRecords = {
    // Construct nodes before relationships, as relationships might depend on nodes
    val nodes = toCreate.collect {
      case c: ConstructedNode if !constructedTable.header.vars.contains(c.v) => c
    }
    val rels = toCreate.collect {
      case r: ConstructedRelationship if !constructedTable.header.vars.contains(r.v) => r
    }

    val (_, createdNodes) = nodes.foldLeft(0 -> Map.empty[Expr, Column]) {
      case ((nextColumnPartitionId, constructedNodes), nextNodeToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedNodes ++ constructNode(newEntityTag, nextColumnPartitionId, nodes.size, nextNodeToConstruct, constructedTable))
    }

    val recordsWithNodes = addEntitiesToRecords(createdNodes, constructedTable)

    val (_, createdRels) = rels.foldLeft(0 -> Map.empty[Expr, Column]) {
      case ((nextColumnPartitionId, constructedRels), nextRelToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedRels ++ constructRel(newEntityTag, nextColumnPartitionId, rels.size, nextRelToConstruct, recordsWithNodes))
    }

    addEntitiesToRecords(createdRels, recordsWithNodes)
  }

  private def addEntitiesToRecords(
    columnsToAdd: Map[Expr, Column],
    constructedTable: CAPSRecords
  ): CAPSRecords = {
    // TODO: Move header construction to FlatPlanner
    val newHeader = constructedTable.header.withExprs(columnsToAdd.keySet)

    val newData = columnsToAdd.foldLeft(constructedTable.df) {
      case (acc, (expr, col)) =>
        acc.safeAddColumn(newHeader.column(expr), col)
    }

    CAPSRecords(newHeader, newData)(constructedTable.caps)
  }

  private def constructNode(
    newEntityTag: Int,
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    node: ConstructedNode,
    constructedTable: CAPSRecords
  ): Map[Expr, Column] = {
    val col = functions.lit(true)

    // TODO: why not Map instead of Set?
    val copiedLabelTuples = node.baseEntity match {
      case Some(origNode) => copyExpressions(node.v, constructedTable)(_.labelsFor(origNode))
      case None => Map.empty
    }

    val labelTuples = node.labels.map { label => HasLabel(node.v, label)(CTBoolean) -> col }.toMap ++ copiedLabelTuples

    val propertyTuples = node.baseEntity match {
      case Some(origNode) => copyExpressions(node.v, constructedTable)(_.propertiesFor(origNode))
      case None => Map.empty
    }

    labelTuples ++ propertyTuples + (node.v -> generateId(columnIdPartition, numberOfColumnPartitions).setTag(newEntityTag))
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
  ): Map[Expr, Column] = {
    val ConstructedRelationship(rel, source, target, typOpt, baseRelOpt) = toConstruct
    val header = constructedTable.header
    val inData = constructedTable.df

    // source and target are present: just copy
    val sourceTuple = {
      val dfColumn = inData.col(header.column(source))
      StartNode(rel)(CTInteger) -> dfColumn
    }
    val targetTuple = {
      val dfColumn = inData.col(header.column(target))
      EndNode(rel)(CTInteger) -> dfColumn
    }

    // id needs to be generated
    val relTuple = rel -> generateId(columnIdPartition, numberOfColumnPartitions).setTag(newEntityTag)

    val typeTuple = {
      typOpt match {
        // type is set
        case Some(t) =>
          val col = functions.lit(t)
          HasType(rel, RelType(t))(CTString) -> col
        case None =>
          // When no type is present, it needs to be a copy of a base relationship
          copyExpressions(rel, constructedTable)(header => Set(header.typeFor(baseRelOpt.get).get)).head
      }
    }

    val propertyTuples: Map[Expr, Column] = baseRelOpt match {
      case Some(baseRel) =>
        copyExpressions(rel, constructedTable)(_.propertiesFor(baseRel))
      case None => Map.empty
    }

    propertyTuples + relTuple + typeTuple + sourceTuple + targetTuple
  }

  private def copyExpressions[T <: Expr](targetVar: Var, records: CAPSRecords)
    (extractor: RecordHeaderNew => Set[T]): Map[Expr, Column] = {
    val header = records.header
    val origExprs = extractor(header)
    val copyExprs = origExprs.map(_.withOwner(targetVar))
    val dfColumns = origExprs.map(header.column).map(records.df.col)
    copyExprs.zip(dfColumns).toMap
  }
}
