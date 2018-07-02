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
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTNode, CTRelationship}
import org.opencypher.okapi.api.value.CypherValue.CypherInteger
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, SchemaException}
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr.{Expr, Var, _}
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.ir.api.{PropertyKey, RelType}
import org.opencypher.okapi.logical.impl.{ConstructedEntity, ConstructedNode, ConstructedRelationship, LogicalPatternGraph, _}
import org.opencypher.okapi.relational.api.physical.PhysicalOperator
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.impl.physical.{CrossJoin, JoinType, _}
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.trees.AbstractTreeNode
import org.opencypher.spark.api.{CAPSSession, Tags}
import org.opencypher.spark.impl.CAPSGraph.EmptyGraph
import org.opencypher.spark.impl.CAPSUnionGraph.{apply => _, unapply => _}
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.SparkSQLExprMapper._
import org.opencypher.spark.impl.physical.CAPSRuntimeContext
import org.opencypher.spark.impl.table.SparkFlatRelationalTable._
import org.opencypher.spark.impl.util.TagSupport.computeRetaggings
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords, CAPSUnionGraph}
import org.opencypher.spark.schema.CAPSSchema._

private[spark] abstract class CAPSPhysicalOperator
  extends AbstractTreeNode[CAPSPhysicalOperator]
    with PhysicalOperator[DataFrameTable, CAPSRecords, CAPSGraph, CAPSRuntimeContext] {

  lazy val records = CAPSRecords(header, table, returnItems.map(_.map(_.withoutType)))

  override def header: RecordHeader = children.head.header

  override def _table: DataFrameTable = children.head.table

  override implicit def context: CAPSRuntimeContext = children.head.context

  implicit def session: CAPSSession = context.session

  override def graph: CAPSGraph = children.head.graph

  override def graphName: QualifiedGraphName = children.head.graphName

  override def returnItems: Option[Seq[Var]] = children.head.returnItems

  protected def resolve(qualifiedGraphName: QualifiedGraphName)(implicit context: CAPSRuntimeContext): CAPSGraph =
    context.resolveGraph(qualifiedGraphName)

  protected def resolveTags(qgn: QualifiedGraphName)(implicit context: CAPSRuntimeContext): Set[Int] = resolve(qgn).tags

  override def args: Iterator[Any] = super.args
}

// Leaf

final case class Start(qgn: QualifiedGraphName, recordsOpt: Option[CAPSRecords])
  (implicit override val context: CAPSRuntimeContext) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = recordsOpt.map(_.header).getOrElse(RecordHeader.empty)

  override lazy val _table: DataFrameTable = recordsOpt.map(_.table).getOrElse(CAPSRecords.unit().table)

  override lazy val graph: CAPSGraph = resolve(qgn)

  override lazy val graphName: QualifiedGraphName = qgn

  override lazy val returnItems: Option[Seq[Var]] = None

  override def toString: String = {
    val graphArg = qgn.toString
    val recordsArg = recordsOpt.map(_.toString)
    val allArgs = List(recordsArg, graphArg).mkString(", ")
    s"Start($allArgs)"
  }

}

// Unary

final case class Cache(in: CAPSPhysicalOperator) extends CAPSPhysicalOperator {

  override lazy val _table: DataFrameTable = context.cache.getOrElse(in, {
    in.table.cache()
    context.cache(in) = in.table
    in.table
  })

}

final case class NodeScan(in: CAPSPhysicalOperator, v: Var) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.graph.schema.headerForNode(v)

  // TODO: replace with NodeVar
  override lazy val _table: DataFrameTable = {
    val nodeTable = in.graph.nodes(v.name, v.cypherType.asInstanceOf[CTNode]).table

    if (header.columns != nodeTable.physicalColumns.toSet) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed columns based on graph schema: ${header.columns.toSeq.sorted.mkString(", ")}
           |  - Actual columns in scan table: ${nodeTable.physicalColumns.sorted.mkString(", ")}
        """.stripMargin)
    }
    nodeTable
  }
}

final case class RelationshipScan(in: CAPSPhysicalOperator, v: Var) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.graph.schema.headerForRelationship(v)

  // TODO: replace with RelationshipVar
  override lazy val _table: DataFrameTable = {
    val relTable = in.graph.relationships(v.name, v.cypherType.asInstanceOf[CTRelationship]).table

    if (header.columns != relTable.physicalColumns.toSet) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed columns based on graph schema: ${header.columns.toSeq.sorted.mkString(", ")}
           |  - Actual columns in scan table: ${relTable.physicalColumns.sorted.mkString(", ")}
        """.stripMargin)
    }
    relTable
  }
}

final case class Alias(in: CAPSPhysicalOperator, aliases: Seq[AliasExpr]) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.header.withAlias(aliases: _*)
}

final case class Add(in: CAPSPhysicalOperator, expr: Expr) extends CAPSPhysicalOperator {

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

  override lazy val _table: DataFrameTable = {
    if (in.header.contains(expr)) {
      in.table
    } else {
      in.table.withColumn(header.column(expr), expr)(header, context.parameters)
    }
  }
}

final case class AddInto(in: CAPSPhysicalOperator, add: Expr, into: Expr) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.header.withExpr(into)

  override lazy val _table: DataFrameTable = in.table.withColumn(header.column(into), add)(header, context.parameters)
}

final case class Drop[T <: Expr](in: CAPSPhysicalOperator, exprs: Set[T]) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.header -- exprs

  private lazy val columnsToDrop = in.header.columns -- header.columns

  override lazy val _table: DataFrameTable = {
    if (columnsToDrop.nonEmpty) {
      in.table.drop(columnsToDrop.toSeq: _*)
    } else {
      in.table
    }
  }
}

final case class RenameColumns(in: CAPSPhysicalOperator, renameExprs: Map[Expr, String]) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = renameExprs.foldLeft(in.header) {
    case (currentHeader, (expr, newColumn)) => currentHeader.withColumnRenamed(expr, newColumn)
  }

  override lazy val _table: DataFrameTable = renameExprs.foldLeft(in.table) {
    case (currentTable, (expr, newColumn)) => currentTable.withColumnRenamed(in.header.column(expr), newColumn)
  }
}

final case class Filter(in: CAPSPhysicalOperator, expr: Expr) extends CAPSPhysicalOperator {

  override lazy val _table: DataFrameTable = in.table.filter(expr)(header, context.parameters)
}

final case class ReturnGraph(in: CAPSPhysicalOperator) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = RecordHeader.empty

  override lazy val _table: DataFrameTable = in.table.empty()
}

final case class Select(in: CAPSPhysicalOperator, expressions: List[Expr]) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = {
    val aliasExprs = expressions.collect { case a: AliasExpr => a }
    val headerWithAliases = in.header.withAlias(aliasExprs: _*)
    headerWithAliases.select(expressions: _*)
  }

  override lazy val _table: DataFrameTable = {
    in.table.select(expressions.map(header.column).distinct: _*)
  }

  override lazy val returnItems: Option[Seq[Var]] = Some(expressions.flatMap(_.owner).collect { case e: Var => e }.distinct)
}

final case class Distinct(in: CAPSPhysicalOperator, fields: Set[Var]) extends CAPSPhysicalOperator {

  override lazy val _table: DataFrameTable = in.table.distinct(fields.flatMap(header.expressionsFor).map(header.column).toSeq: _*)

}

final case class SimpleDistinct(in: CAPSPhysicalOperator) extends CAPSPhysicalOperator {

  override lazy val _table: DataFrameTable = in.table.distinct
}

final case class Aggregate(
  in: CAPSPhysicalOperator,
  aggregations: Set[(Var, Aggregator)],
  group: Set[Var]
) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.header.select(group).withExprs(aggregations.map(_._1))

  override lazy val _table: DataFrameTable = {
    val preparedAggregations = aggregations.map { case (v, agg) => agg -> (header.column(v) -> v.cypherType) }
    in.table.group(group, preparedAggregations)(in.header, context.parameters)
  }
}

final case class OrderBy(in: CAPSPhysicalOperator, sortItems: Seq[SortItem[Expr]]) extends CAPSPhysicalOperator {

  override lazy val _table: DataFrameTable = {
    val tableSortItems: Seq[(String, Order)] = sortItems.map {
      case Asc(expr) => header.column(expr) -> Ascending
      case Desc(expr) => header.column(expr) -> Descending
    }
    in.table.orderBy(tableSortItems: _*)
  }
}

final case class Skip(in: CAPSPhysicalOperator, expr: Expr) extends CAPSPhysicalOperator {

  override lazy val _table: DataFrameTable = {
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

  override lazy val _table: DataFrameTable = {
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

  override lazy val _table: DataFrameTable = in.table.empty(header)
}

final case class FromGraph(in: CAPSPhysicalOperator, logicalGraph: LogicalCatalogGraph) extends CAPSPhysicalOperator {

  override def graph: CAPSGraph = resolve(logicalGraph.qualifiedGraphName)

  override def graphName: QualifiedGraphName = logicalGraph.qualifiedGraphName

}

case class RetagVariable(in: CAPSPhysicalOperator, v: Var, replacements: Map[Int, Int]) extends CAPSPhysicalOperator {

  override lazy val _table: DataFrameTable = {
    val columnsToUpdate = header.idColumns(v)
    // TODO: implement efficiently for multiple columns
    columnsToUpdate.foldLeft(in.table.df) { case (currentDf, columnName) =>
      currentDf.safeReplaceTags(columnName, replacements)
    }
  }
}

final case class AddEntitiesToRecords(
  in: CAPSPhysicalOperator,
  columnsToAdd: Map[Expr, Column]
) extends CAPSPhysicalOperator {

  override lazy val header: RecordHeader = in.header.withExprs(columnsToAdd.keySet)

  override lazy val _table: DataFrameTable = {
    val dfWithColumns = columnsToAdd.foldLeft(in.table.df) {
      case (acc, (expr, col)) => acc.safeAddColumn(header.column(expr), col)
    }
    dfWithColumns.setNullability(columnsToAdd.keys.map(e => header.column(e) -> e.cypherType).toMap)
  }

}

final case class ConstructProperty(in: CAPSPhysicalOperator, v: Var, propertyExpr: Property, valueExpr: Expr)
  (implicit context: CAPSRuntimeContext) extends CAPSPhysicalOperator {

  private lazy val existingPropertyExpressionsForKey = in.header.propertiesFor(v).collect({
    case p@Property(_, PropertyKey(name)) if name == propertyExpr.key.name => p
  })

  override lazy val header: RecordHeader = {
    val inHeader = in.header

    val headerWithExistingRemoved = inHeader -- existingPropertyExpressionsForKey
    headerWithExistingRemoved.withExpr(propertyExpr)
  }

  override lazy val _table: DataFrameTable = {
    val inHeader = in.header
    val inTable = in.table

    val propertyValueColumn: Column = valueExpr.asSparkSQLExpr(inHeader, inTable.df, context.parameters)


    val dataWithExistingRemoved = existingPropertyExpressionsForKey.foldLeft(inTable.df) {
      case (acc, toRemove) => acc.safeDropColumn(inHeader.column(toRemove))
    }

    dataWithExistingRemoved.safeAddColumn(header.column(propertyExpr), propertyValueColumn)
  }
}

// Binary

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

    val aliasOp: CAPSPhysicalOperator = Alias(lhs, aliasClones.map { case (expr, alias) => expr as alias }.toSeq)

    val retagBaseTableOp = clonedVarsToInputVars.foldLeft(aliasOp) {
      case (op, (alias, original)) => RetagVariable(op, alias, constructTagStrategy(original.cypherType.graph.get))
    }

    // Construct NEW entities
    val (newEntityTags, constructedEntitiesOp) = {
      if (newEntities.isEmpty) {
        Set.empty[Int] -> retagBaseTableOp
      } else {
        val newEntityTag = pickFreeTag(constructTagStrategy)
        val entitiesOp = createEntities(retagBaseTableOp, newEntities, newEntityTag)

        val entityTableWithProperties = sets.foldLeft(entitiesOp) {
          case (currentOp, SetPropertyItem(propertyKey, v, valueExpr)) =>
            val propertyExpression = Property(v, PropertyKey(propertyKey))(valueExpr.cypherType)
            ConstructProperty(currentOp, v, propertyExpression, valueExpr)
        }
        Set(newEntityTag) -> entityTableWithProperties
      }
    }

    // Remove all vars that were part the original pattern graph DF, except variables that were CLONEd without an alias
    val allInputVars = aliasOp.header.vars
    val originalVarsToKeep = clonedVarsToInputVars.keySet -- aliasClones.keySet
    val varsToRemoveFromTable = allInputVars -- originalVarsToKeep

    val patternGraphTable = Drop(constructedEntitiesOp, varsToRemoveFromTable)

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

    context.queryCatalog.update(construct.name, constructedCombinedWithOn)

    (constructedCombinedWithOn, name, constructTagStrategy)
  }

  private def createEntities(
    inOp: CAPSPhysicalOperator,
    toCreate: Set[ConstructedEntity],
    newEntityTag: Int
  ): CAPSPhysicalOperator = {

    // Construct nodes before relationships, as relationships might depend on nodes
    val nodes: Set[ConstructedNode] = toCreate.collect {
      case c: ConstructedNode if !inOp.header.vars.contains(c.v) => c
    }
    val rels: Set[ConstructedRelationship] = toCreate.collect {
      case r: ConstructedRelationship if !inOp.header.vars.contains(r.v) => r
    }

    val (_, nodesToCreate) = nodes.foldLeft(0 -> Map.empty[Expr, Column]) {
      case ((nextColumnPartitionId, constructedNodes), nextNodeToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedNodes ++ constructNode(inOp, newEntityTag, nextColumnPartitionId, nodes.size, nextNodeToConstruct))
    }

    val createdNodesOp = AddEntitiesToRecords(inOp, nodesToCreate)

    val (_, relsToCreate) = rels.foldLeft(0 -> Map.empty[Expr, Column]) {
      case ((nextColumnPartitionId, constructedRels), nextRelToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedRels ++ constructRel(createdNodesOp, newEntityTag, nextColumnPartitionId, rels.size, nextRelToConstruct))
    }

    AddEntitiesToRecords(createdNodesOp, relsToCreate)
  }

  private def constructNode(
    inOp: CAPSPhysicalOperator,
    newEntityTag: Int,
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    node: ConstructedNode
  ): Map[Expr, Column] = {

    val idTuple = node.v -> generateId(columnIdPartition, numberOfColumnPartitions).setTag(newEntityTag)

    val copiedLabelTuples = node.baseEntity match {
      case Some(origNode) => copyExpressions(inOp, node.v)(_.labelsFor(origNode))
      case None => Map.empty
    }

    val newLabelTuples = node.labels.map {
      label => HasLabel(node.v, label)(CTBoolean) -> functions.lit(true)
    }.toMap

    val propertyTuples = node.baseEntity match {
      case Some(origNode) => copyExpressions(inOp, node.v)(_.propertiesFor(origNode))
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
    inOp: CAPSPhysicalOperator,
    newEntityTag: Int,
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    toConstruct: ConstructedRelationship
  ): Map[Expr, Column] = {
    val ConstructedRelationship(rel, source, target, typOpt, baseRelOpt) = toConstruct
    val header = inOp.header
    val inData = inOp.table.df

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
          copyExpressions(inOp, rel)(_.typesFor(baseRelOpt.get))
      }
    }

    val propertyTuples: Map[Expr, Column] = baseRelOpt match {
      case Some(baseRel) =>
        copyExpressions(inOp, rel)(_.propertiesFor(baseRel))
      case None => Map.empty
    }

    propertyTuples ++ typeTuple + idTuple + sourceTuple + targetTuple
  }

  private def copyExpressions[T <: Expr](inOp: CAPSPhysicalOperator, targetVar: Var)
    (extractor: RecordHeader => Set[T]): Map[Expr, Column] = {
    val origExprs = extractor(inOp.header)
    val copyExprs = origExprs.map(_.withOwner(targetVar))
    val dfColumns = origExprs.map(inOp.header.column).map(inOp.table.df.col)
    copyExprs.zip(dfColumns).toMap
  }
}

// N-ary

final case class GraphUnionAll(inputs: List[CAPSPhysicalOperator], qgn: QualifiedGraphName)
  extends CAPSPhysicalOperator {
  require(inputs.nonEmpty, "GraphUnionAll requires at least one input")

  override lazy val tagStrategy = {
    computeRetaggings(inputs.map(r => r.graphName -> r.graph.tags).toMap)
  }

  override lazy val graphName = qgn

  override lazy val graph = {
    val graphWithTagStrategy = inputs.map(i => i.graph -> tagStrategy(i.graphName)).toMap
    CAPSUnionGraph(graphWithTagStrategy)
  }

}
