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
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, Var, _}
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.ir.api.{PropertyKey, RelType}
import org.opencypher.okapi.logical.impl.{ConstructedEntity, ConstructedNode, ConstructedRelationship, LogicalPatternGraph}
import org.opencypher.okapi.relational.impl.physical.{CrossJoin, JoinType}
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.Tags
import org.opencypher.spark.api.io.SparkCypherTable.DataFrameTable
import org.opencypher.spark.impl.CAPSGraph.EmptyGraph
import org.opencypher.spark.impl.CAPSUnionGraph.{apply => _, unapply => _}
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.impl.physical.CAPSRuntimeContext
import org.opencypher.spark.impl.util.TagSupport._
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords, CAPSUnionGraph}
import org.opencypher.spark.schema.CAPSSchema._

final case class Join(
  lhs: CAPSPhysicalOperator,
  rhs: CAPSPhysicalOperator,
  joinExprs: Seq[(Expr, Expr)] = Seq.empty,
  joinType: JoinType = CrossJoin
) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = lhs.header join rhs.header

  override lazy val _table: DataFrameTable = {

    val lhsTable = lhs.table
    val rhsTable = rhs.table

    // TODO: move conflict resolution to relational planner
    val conflictFreeRhs = if (lhsTable.physicalColumns.toSet ++ rhsTable.physicalColumns.toSet != header.columns) {
      val renameColumns = rhs.header.expressions
        .filter(expr => rhs.header.column(expr) != header.column(expr))
        .map { expr => expr -> header.column(expr) }.toSeq
      RenameColumns(rhs, renameColumns.toMap)
    } else {
      rhs
    }

    val joinCols = joinExprs.map { case (l, r) => header.column(l) -> conflictFreeRhs.header.column(r) }
    lhs.table.join(conflictFreeRhs.table, joinType, joinCols: _*)
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
// TODO: rename to UnionByName
// TODO: refactor to n-ary operator (i.e. take List[PhysicalOperator] as input)
final case class TabularUnionAll(lhs: CAPSPhysicalOperator, rhs: CAPSPhysicalOperator) extends CAPSPhysicalOperator {

  override lazy val _table: DataFrameTable = {
    val lhsTable = lhs.table
    val rhsTable = rhs.table

    val leftColumns = lhsTable.physicalColumns
    val rightColumns = rhsTable.physicalColumns

    if (leftColumns.size != rightColumns.size) {
      throw IllegalArgumentException("same number of columns", s"left: $leftColumns right: $rightColumns")
    }
    if (leftColumns.toSet != rightColumns.toSet) {
      throw IllegalArgumentException("same column names", s"left: $leftColumns right: $rightColumns")
    }

    val orderedRhsTable = if (leftColumns != rightColumns) {
      rhsTable.select(leftColumns: _*)
    } else {
      rhsTable
    }

    lhsTable.unionAll(orderedRhsTable)
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
) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = RecordHeader.empty

  override lazy val _table: DataFrameTable = lhs.table.unit

  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }


  private def pickFreeTag(tagStrategy: Map[QualifiedGraphName, Map[Int, Int]]): Int = {
    val usedTags = tagStrategy.values.flatMap(_.values).toSet
    Tags.pickFreeTag(usedTags)
  }

  private def identityRetaggings(g: CAPSGraph): (CAPSGraph, Map[Int, Int]) = {
    g -> g.tags.zip(g.tags).toMap
  }

  override lazy val (graph, graphName, tagStrategy): (CAPSGraph, QualifiedGraphName, TagStrategy) = {

    val onGraph = rhs.graph

    val unionTagStrategy: Map[QualifiedGraphName, Map[Int, Int]] = rhs.tagStrategy

    val LogicalPatternGraph(schema, clonedVarsToInputVars, newEntities, sets, _, name) = construct

    val matchGraphs: Set[QualifiedGraphName] = clonedVarsToInputVars.values.map(_.cypherType.graph.get).toSet
    val allGraphs = unionTagStrategy.keySet ++ matchGraphs
    val tagsForGraph: Map[QualifiedGraphName, Set[Int]] = allGraphs.map(qgn => qgn -> resolveTags(qgn)).toMap

    val constructTagStrategy = computeRetaggings(tagsForGraph, unionTagStrategy)

    // Apply aliases in CLONE to input table in order to create the base table, on which CONSTRUCT happens
    val aliasClones = clonedVarsToInputVars
      .filter { case (alias, original) => alias != original }
      .map(_.swap)


    val aliasedLhs = Alias(lhs, aliasClones.map { case (expr, alias) => expr as alias }.toSeq)
    val baseTableHeader = aliasedLhs.header
    val baseTable = aliasedLhs.table

    val retaggedBaseTable = clonedVarsToInputVars.foldLeft(baseTable) { case (df, clone) =>
      df.retagColumn(constructTagStrategy(clone._2.cypherType.graph.get), baseTableHeader.column(clone._1))
    }

    // Construct NEW entities
    val (newEntityTags, (headerWithConstructedEntities, tableWithConstructedEntities)) = {
      if (newEntities.isEmpty) {
        Set.empty[Int] -> (baseTableHeader -> retaggedBaseTable)
      } else {
        val newEntityTag = pickFreeTag(constructTagStrategy)
        val (entityHeader, entityTable) = createEntities(newEntities, baseTableHeader, retaggedBaseTable, newEntityTag)
        val entityTableWithProperties = sets.foldLeft(entityHeader -> entityTable) {
          case ((currentHeader, currentTable), SetPropertyItem(key, v, expr)) =>
            constructProperty(v, key, expr, currentHeader, currentTable)
        }
        Set(newEntityTag) -> entityTableWithProperties
      }
    }

    // Remove all vars that were part the original pattern graph DF, except variables that were CLONEd without an alias
    val allInputVars = aliasedLhs.header.vars
    val originalVarsToKeep = clonedVarsToInputVars.keySet -- aliasClones.keySet
    val varsToRemoveFromTable = allInputVars -- originalVarsToKeep

    val recordsWithConstructedEntities = CAPSRecords(headerWithConstructedEntities, tableWithConstructedEntities.df)
    val patternGraphTable = DropColumns(Start(context.session.emptyGraphQgn, Some(recordsWithConstructedEntities)), varsToRemoveFromTable)


    val tagsUsed = constructTagStrategy.foldLeft(newEntityTags) {
      case (tags, (qgn, remapping)) =>
        val remappedTags = tagsForGraph(qgn).map(remapping)
        tags ++ remappedTags
    }

    val patternRecords = CAPSRecords(patternGraphTable.header, patternGraphTable.table.df)
    val patternGraph = CAPSGraph.create(patternRecords, schema.asCaps, tagsUsed)

    val constructedCombinedWithOn = onGraph match {
      case _: EmptyGraph => CAPSUnionGraph(Map(identityRetaggings(patternGraph)))
      case _ => CAPSUnionGraph(Map(identityRetaggings(onGraph), identityRetaggings(patternGraph)))
    }

    context.patternGraphTags.update(construct.name, constructedCombinedWithOn.tags)

    (patternGraph, name, constructTagStrategy)
  }

  def constructProperty(
    variable: Var,
    propertyKey: String,
    propertyValue: Expr,
    constructedHeader: RecordHeader,
    constructedTable: DataFrameTable
  )(implicit context: CAPSRuntimeContext): (RecordHeader, DataFrameTable) = {
    val propertyValueColumn: Column = propertyValue.asSparkSQLExpr(constructedHeader, constructedTable.df, context.parameters)

    val propertyExpression = Property(variable, PropertyKey(propertyKey))(propertyValue.cypherType)

    val existingPropertyExpressionsForKey = constructedHeader.propertiesFor(variable).collect({
      case p@Property(_, PropertyKey(name)) if name == propertyKey => p
    })

    val headerWithExistingRemoved = constructedHeader -- existingPropertyExpressionsForKey
    val dataWithExistingRemoved = existingPropertyExpressionsForKey.foldLeft(constructedTable.df) {
      case (acc, toRemove) => acc.safeDropColumn(constructedHeader.column(toRemove))
    }

    val newHeader = headerWithExistingRemoved.withExpr(propertyExpression)
    val newData = dataWithExistingRemoved.safeAddColumn(newHeader.column(propertyExpression), propertyValueColumn)

    newHeader -> newData
  }

  private def createEntities(
    toCreate: Set[ConstructedEntity],
    constructedHeader: RecordHeader,
    constructedTable: DataFrameTable,
    newEntityTag: Int
  ): (RecordHeader, DataFrameTable) = {
    // Construct nodes before relationships, as relationships might depend on nodes
    val nodes = toCreate.collect {
      case c: ConstructedNode if !constructedHeader.vars.contains(c.v) => c
    }
    val rels = toCreate.collect {
      case r: ConstructedRelationship if !constructedHeader.vars.contains(r.v) => r
    }

    val (_, createdNodes) = nodes.foldLeft(0 -> Map.empty[Expr, Column]) {
      case ((nextColumnPartitionId, constructedNodes), nextNodeToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedNodes ++ constructNode(newEntityTag, nextColumnPartitionId, nodes.size, nextNodeToConstruct, constructedHeader, constructedTable))
    }

    val (headerWithNodes, tableWithNodes) = addEntitiesToRecords(createdNodes, constructedHeader, constructedTable)

    val (_, createdRels) = rels.foldLeft(0 -> Map.empty[Expr, Column]) {
      case ((nextColumnPartitionId, constructedRels), nextRelToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedRels ++ constructRel(newEntityTag, nextColumnPartitionId, rels.size, nextRelToConstruct, headerWithNodes, tableWithNodes))
    }

    addEntitiesToRecords(createdRels, headerWithNodes, tableWithNodes)
  }

  // TODO: refactor to own physical operator
  private def addEntitiesToRecords(
    columnsToAdd: Map[Expr, Column],
    constructedHeader: RecordHeader,
    constructedTable: DataFrameTable
  ): (RecordHeader, DataFrameTable) = {
    // TODO: Move header construction to FlatPlanner
    val newHeader = constructedHeader.withExprs(columnsToAdd.keySet)

    val newData = columnsToAdd.foldLeft(constructedTable.df) {
      case (acc, (expr, col)) =>
        acc.safeAddColumn(newHeader.column(expr), col)
    }

    newHeader -> newData
  }

  private def constructNode(
    newEntityTag: Int,
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    node: ConstructedNode,
    constructedTableHeader: RecordHeader,
    constructedTable: DataFrameTable
  ): Map[Expr, Column] = {

    val idTuple = node.v -> generateId(columnIdPartition, numberOfColumnPartitions).setTag(newEntityTag)

    val copiedLabelTuples = node.baseEntity match {
      case Some(origNode) => copyExpressions(node.v, constructedTableHeader, constructedTable)(_.labelsFor(origNode))
      case None => Map.empty
    }

    val newLabelTuples = node.labels.map {
      label => HasLabel(node.v, label)(CTBoolean) -> functions.lit(true)
    }.toMap

    val propertyTuples = node.baseEntity match {
      case Some(origNode) => copyExpressions(node.v, constructedTableHeader, constructedTable)(_.propertiesFor(origNode))
      case None => Map.empty
    }

    newLabelTuples ++
      copiedLabelTuples ++
      propertyTuples +
      idTuple
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
    constructedTableHeader: RecordHeader,
    constructedTable: DataFrameTable
  ): Map[Expr, Column] = {
    val ConstructedRelationship(rel, source, target, typOpt, baseRelOpt) = toConstruct
    val header = constructedTableHeader
    val inData = constructedTable.df

    // id needs to be generated
    val idTuple = rel -> generateId(columnIdPartition, numberOfColumnPartitions).setTag(newEntityTag)

    // source and target are present: just copy
    val sourceTuple = {
      val dfColumn = inData.col(header.column(source))
      StartNode(rel)(CTInteger) -> dfColumn
    }
    val targetTuple = {
      val dfColumn = inData.col(header.column(target))
      EndNode(rel)(CTInteger) -> dfColumn
    }

    val typeTuple: Map[Expr, Column] = {
      typOpt match {
        // type is set
        case Some(t) =>
          Map(HasType(rel, RelType(t))(CTBoolean) -> functions.lit(true))
        case None =>
          // When no type is present, it needs to be a copy of a base relationship
          copyExpressions(rel, constructedTableHeader, constructedTable)(_.typesFor(baseRelOpt.get))
      }
    }

    val propertyTuples: Map[Expr, Column] = baseRelOpt match {
      case Some(baseRel) =>
        copyExpressions(rel, constructedTableHeader, constructedTable)(_.propertiesFor(baseRel))
      case None => Map.empty
    }

    propertyTuples ++ typeTuple + idTuple + sourceTuple + targetTuple
  }

  private def copyExpressions[T <: Expr](targetVar: Var, header: RecordHeader, records: DataFrameTable)
    (extractor: RecordHeader => Set[T]): Map[Expr, Column] = {
    val origExprs = extractor(header)
    val copyExprs = origExprs.map(_.withOwner(targetVar))
    val dfColumns = origExprs.map(header.column).map(records.df.col)
    copyExprs.zip(dfColumns).toMap
  }
}
