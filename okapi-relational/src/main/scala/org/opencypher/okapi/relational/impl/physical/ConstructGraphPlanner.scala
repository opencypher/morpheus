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
package org.opencypher.okapi.relational.impl.physical

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.ir.api.{PropertyKey, RelType}
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.physical.{RelationalPlannerContext, RelationalRuntimeContext}
import org.opencypher.okapi.relational.api.table.FlatRelationalTable
import org.opencypher.okapi.relational.api.tagging.TagSupport.computeRetaggings
import org.opencypher.okapi.relational.api.tagging.Tags
import org.opencypher.okapi.relational.api.tagging.Tags._
import org.opencypher.okapi.relational.impl.operators.RelationalOperator
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.relational.impl.{operators => relational}

object ConstructGraphPlanner {

  def planConstructGraph[T <: FlatRelationalTable[T]](in: Option[LogicalOperator], construct: LogicalPatternGraph)
    (
      implicit plannerContext: RelationalPlannerContext[T],
      runtimeContext: RelationalRuntimeContext[T]
    ): RelationalOperator[T] = {

    val onGraphPlan: RelationalOperator[T] = {
      construct.onGraphs match {
        case Nil => relational.Start[T](plannerContext.session.emptyGraphQgn) // Empty start
        //TODO: Optimize case where no union is necessary
        //case h :: Nil => operatorProducer.planStart(Some(h)) // Just one graph, no union required
        case several =>
          val onGraphPlans = several.map(qgn => relational.Start[T](qgn))
          relational.GraphUnionAll[T](onGraphPlans, construct.name)
      }
    }
    val inputTablePlan = in.map(RelationalPlanner.process(_)(plannerContext, runtimeContext)).getOrElse(relational.Start[T](plannerContext.session.emptyGraphQgn))

    val constructGraphPlan = ConstructGraph(inputTablePlan, onGraphPlan, construct)

    plannerContext.constructedGraphPlans += (construct.name -> constructGraphPlan)
    constructGraphPlan
  }
}

final case class ConstructGraph[T <: FlatRelationalTable[T]](
  lhs: RelationalOperator[T],
  rhs: RelationalOperator[T],
  construct: LogicalPatternGraph
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = RecordHeader.empty

  override lazy val _table: T = session.records.unit().table

  override def toString: String = {
    val entities = construct.clones.keySet ++ construct.newEntities.map(_.v)
    s"ConstructGraph(on=[${construct.onGraphs.mkString(", ")}], entities=[${entities.mkString(", ")}])"
  }

  private def pickFreeTag(tagStrategy: Map[QualifiedGraphName, Map[Int, Int]]): Int = {
    val usedTags = tagStrategy.values.flatMap(_.values).toSet
    Tags.pickFreeTag(usedTags)
  }

  private def identityRetaggings(g: RelationalCypherGraph[T]): (RelationalCypherGraph[T], Map[Int, Int]) = {
    g -> g.tags.zip(g.tags).toMap
  }

  override lazy val (graph, graphName, tagStrategy): (RelationalCypherGraph[T], QualifiedGraphName, TagStrategy) = {

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

    val aliasOp: RelationalOperator[T] = if (aliasClones.isEmpty) {
      lhs
    } else {
      relational.Alias(lhs, aliasClones.map { case (expr, alias) => expr as alias }.toSeq)
    }

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

    val patternGraphTableOp = if (varsToRemoveFromTable.isEmpty) {
      constructedEntitiesOp
    } else {
      relational.Drop(constructedEntitiesOp, varsToRemoveFromTable)
    }

    val tagsUsed = constructTagStrategy.foldLeft(newEntityTags) {
      case (tags, (qgn, remapping)) =>
        val remappedTags = tagsForGraph(qgn).map(remapping)
        tags ++ remappedTags
    }

    val patternGraphRecords = session.records.from(patternGraphTableOp.header, patternGraphTableOp.table)

    val patternGraph = session.graphs.singleTableGraph(patternGraphRecords, schema, tagsUsed)

    val constructedCombinedWithOn = if (onGraph == session.graphs.empty) {
      session.graphs.unionGraph(patternGraph)
    } else {
      session.graphs.unionGraph(Map(identityRetaggings(onGraph), identityRetaggings(patternGraph)))
    }

    context.constructedGraphCatalog += (construct.name -> constructedCombinedWithOn)

    (constructedCombinedWithOn, name, constructTagStrategy)
  }

  private def createEntities(
    inOp: RelationalOperator[T],
    toCreate: Set[ConstructedEntity],
    newEntityTag: Int
  ): RelationalOperator[T] = {

    // Construct nodes before relationships, as relationships might depend on nodes
    val nodes: Set[ConstructedNode] = toCreate.collect {
      case c: ConstructedNode if !inOp.header.vars.contains(c.v) => c
    }
    val rels: Set[ConstructedRelationship] = toCreate.collect {
      case r: ConstructedRelationship if !inOp.header.vars.contains(r.v) => r
    }

    val (_, nodesToCreate) = nodes.foldLeft(0 -> Map.empty[Expr, Expr]) {
      case ((nextColumnPartitionId, constructedNodes), nextNodeToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedNodes ++ constructNode(inOp, newEntityTag, nextColumnPartitionId, nodes.size, nextNodeToConstruct))
    }

    val createdNodesOp = AddEntitiesToRecords(inOp, nodesToCreate)

    val (_, relsToCreate) = rels.foldLeft(0 -> Map.empty[Expr, Expr]) {
      case ((nextColumnPartitionId, constructedRels), nextRelToConstruct) =>
        (nextColumnPartitionId + 1) -> (constructedRels ++ constructRel(createdNodesOp, newEntityTag, nextColumnPartitionId, rels.size, nextRelToConstruct))
    }

    AddEntitiesToRecords(createdNodesOp, relsToCreate)
  }

  private def constructNode(
    inOp: RelationalOperator[T],
    newEntityTag: Int,
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    node: ConstructedNode
  ): Map[Expr, Expr] = {

    val idTuple = node.v -> generateId(columnIdPartition, numberOfColumnPartitions).setTag(newEntityTag)

    val copiedLabelTuples = node.baseEntity match {
      case Some(origNode) => copyExpressions(inOp, node.v)(_.labelsFor(origNode))
      case None => Map.empty
    }

    val newLabelTuples = node.labels.map {
      label => HasLabel(node.v, label)(CTBoolean) -> TrueLit
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
  private def generateId(columnIdPartition: Int, numberOfColumnPartitions: Int): Expr = {
    val columnPartitionBits = math.log(numberOfColumnPartitions).floor.toInt + 1
    val totalIdSpaceBits = 33
    val columnIdShift = totalIdSpaceBits - columnPartitionBits

    // id needs to be generated
    // Limits the system to 500 mn partitions
    // The first half of the id space is protected
    val columnPartitionOffset = columnIdPartition.toLong << columnIdShift

    Add(MonotonicallyIncreasingId(), IntegerLit(columnPartitionOffset)(CTInteger))(CTInteger)
    // TODO: remove line
    //    monotonically_increasing_id() + functions.lit(columnPartitionOffset)
  }

  private def constructRel(
    inOp: RelationalOperator[T],
    newEntityTag: Int,
    columnIdPartition: Int,
    numberOfColumnPartitions: Int,
    toConstruct: ConstructedRelationship
  ): Map[Expr, Expr] = {
    val ConstructedRelationship(rel, source, target, typOpt, baseRelOpt) = toConstruct
//    val header = inOp.header
//    val inTable = inOp.table

    // id needs to be generated
    val idTuple = rel -> generateId(columnIdPartition, numberOfColumnPartitions).setTag(newEntityTag)

    // source and target are present: just copy
    val sourceTuple = {
      //      val dfColumn = inTable.col(header.column(source))
      StartNode(rel)(CTInteger) -> source
    }
    val targetTuple = {
      //      val dfColumn = inTable.col(header.column(target))
      EndNode(rel)(CTInteger) -> target
    }

    val typeTuple: Map[Expr, Expr] = {
      typOpt match {
        // type is set
        case Some(t) =>
          Map(HasType(rel, RelType(t))(CTBoolean) -> TrueLit)
        case None =>
          // When no type is present, it needs to be a copy of a base relationship
          copyExpressions(inOp, rel)(_.typesFor(baseRelOpt.get))
      }
    }

    val propertyTuples: Map[Expr, Expr] = baseRelOpt match {
      case Some(baseRel) =>
        copyExpressions(inOp, rel)(_.propertiesFor(baseRel))
      case None => Map.empty
    }

    propertyTuples ++ typeTuple + idTuple + sourceTuple + targetTuple
  }

  private def copyExpressions[E <: Expr](inOp: RelationalOperator[T], targetVar: Var)
    (extractor: RecordHeader => Set[E]): Map[Expr, Expr] = {
    val origExprs = extractor(inOp.header)
    val copyExprs = origExprs.map(_.withOwner(targetVar))
    //    val dfColumns = origExprs.map(inOp.header.column).map(inOp.table.df.col)
    copyExprs.zip(origExprs).toMap
  }
}

case class RetagVariable[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  v: Var,
  replacements: Map[Int, Int]
) extends RelationalOperator[T] {

  override lazy val _table: T = {
    val expressionsToRetag = header.idExpressions(v)

    // TODO: implement efficiently for multiple columns
    expressionsToRetag.foldLeft(in.table) {
      case (currentTable, exprToRetag) =>
        currentTable.withColumn(header.column(exprToRetag), exprToRetag.replaceTags(replacements))(header, context.parameters)
    }
  }
}

final case class AddEntitiesToRecords[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  exprsToAdd: Map[Expr, Expr]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.header.withExprs(exprsToAdd.keySet)

  override lazy val _table: T = {
    exprsToAdd.foldLeft(in.table) {
      case (acc, (toAdd, value)) => acc.withColumn(header.column(toAdd), value)(header, context.parameters)
    }
  }
}

final case class ConstructProperty[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  v: Var,
  propertyExpr: Property,
  valueExpr: Expr
)
  (implicit context: RelationalRuntimeContext[T]) extends RelationalOperator[T] {

  private lazy val existingPropertyExpressionsForKey = in.header.propertiesFor(v).collect {
    case p@Property(_, PropertyKey(name)) if name == propertyExpr.key.name => p
  }

  override lazy val header: RecordHeader = {
    val headerWithExistingRemoved = in.header -- existingPropertyExpressionsForKey
    headerWithExistingRemoved.withExpr(propertyExpr)
  }

  override lazy val _table: T = {
    in.table
      .drop(existingPropertyExpressionsForKey.map(in.header.column).toSeq: _*)
      .withColumn(header.column(propertyExpr), valueExpr)(in.header, context.parameters)
  }
}
