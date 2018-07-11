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

import org.opencypher.okapi.api.graph.CypherSession
import org.opencypher.okapi.api.types.{CTBoolean, CTNode}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.logical.{impl => logical}
import org.opencypher.okapi.relational.api.physical.{RelationalPlannerContext, RelationalRuntimeContext}
import org.opencypher.okapi.relational.api.table.FlatRelationalTable
import org.opencypher.okapi.relational.impl.operators.{RelationalOperator, RenameColumns}
import org.opencypher.okapi.relational.impl.physical.ConstructGraphPlanner._
import org.opencypher.okapi.relational.impl.{operators => relational}

object RelationalPlanner {

  // TODO: rename to 'plan'
  def process[T <: FlatRelationalTable[T]](input: LogicalOperator)(
    // TODO: unify contexts?
    implicit plannerContext: RelationalPlannerContext[T],
    runtimeContext: RelationalRuntimeContext[T]): RelationalOperator[T] = {

    implicit val caps: CypherSession = plannerContext.session

    input match {
      case logical.CartesianProduct(lhs, rhs, _) =>
        planJoin(process[T](lhs), process[T](rhs), Seq.empty, CrossJoin)

      case logical.Select(fields, in, _) =>

        val inOp = process[T](in)

        val selectExpressions = fields
          .flatMap(inOp.header.ownedBy)
          .distinct

        relational.Select(process[T](in), selectExpressions)

      case logical.Project(projectExpr, in, _) =>
        val inOp = process[T](in)
        val (expr, maybeAlias) = projectExpr
        val containsExpr = inOp.header.contains(expr)

        maybeAlias match {
          case Some(alias) if containsExpr => relational.Alias(inOp, Seq(expr as alias))
          case Some(alias) => relational.Add(inOp, expr as alias)
          case None => relational.Add(inOp, expr)
        }

      case logical.EmptyRecords(fields, in, _) =>
        relational.EmptyRecords(process[T](in), fields)

      case logical.Start(graph, _) => planStart(graph)

      case logical.FromGraph(graph, in, _) =>
        graph match {
          case g: LogicalCatalogGraph =>
            relational.FromGraph(process[T](in), g)

          case construct: LogicalPatternGraph =>
            planConstructGraph(Some(in), construct)//(plannerContext, runtimeContext)
        }

      case logical.Unwind(list, item, in, _) =>
        val explodeExpr = Explode(list)(item.cypherType)
        relational.Add(process[T](in), explodeExpr as item)

      case logical.NodeScan(v, in, _) => relational.NodeScan(process[T](in), v)

      case logical.Aggregate(aggregations, group, in, _) => relational.Aggregate(process[T](in), group, aggregations)

      case logical.Filter(expr, in, _) => expr match {
        case TrueLit =>
          process[T](in) // optimise away filter
        case _ =>
          relational.Filter(process[T](in), expr)
      }

      case logical.ValueJoin(lhs, rhs, predicates, _) =>
        val joinExpressions = predicates.map(p => p.lhs -> p.rhs).toSeq
        planJoin(process[T](lhs), process[T](rhs), joinExpressions, InnerJoin)

      case logical.Distinct(fields, in, _) =>
        val entityExprs: Set[Var] = Set(fields.toSeq: _*)
        relational.Distinct(process[T](in), entityExprs)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case logical.Expand(source, rel, target, direction, sourceOp, targetOp, _) =>
        val first = process[T](sourceOp)
        val third = process[T](targetOp)

        val startFrom = sourceOp.graph match {
          case e: LogicalCatalogGraph =>
            relational.Start(e.qualifiedGraphName)

          case c: LogicalPatternGraph =>
            plannerContext.constructedGraphPlans(c.name)
        }

        val second = relational.RelationshipScan(startFrom, rel)
        val startNode = StartNode(rel)(CTNode)
        val endNode = EndNode(rel)(CTNode)

        direction match {
          case Directed =>
            val tempResult = planJoin(first, second, Seq(source -> startNode), InnerJoin)
            planJoin(tempResult, third, Seq(endNode -> target), InnerJoin)

          case Undirected =>
            val tempOutgoing = planJoin(first, second, Seq(source -> startNode), InnerJoin)
            val outgoing = planJoin(tempOutgoing, third, Seq(endNode -> target), InnerJoin)

            val filterExpression = Not(Equals(startNode, endNode)(CTBoolean))(CTBoolean)
            val relsWithoutLoops = relational.Filter(second, filterExpression)

            val tempIncoming = planJoin(third, relsWithoutLoops, Seq(target -> startNode), InnerJoin)
            val incoming = planJoin(tempIncoming, first, Seq(endNode -> source), InnerJoin)

            relational.TabularUnionAll(outgoing, incoming)
        }

      case logical.ExpandInto(source, rel, target, direction, sourceOp, _) =>
        val in = process[T](sourceOp)
        val relationships = relational.RelationshipScan(in, rel)

        val startNode = StartNode(rel)()
        val endNode = EndNode(rel)()

        direction match {
          case Directed =>
            planJoin(in, relationships, Seq(source -> startNode, target -> endNode), InnerJoin)

          case Undirected =>
            val outgoing = planJoin(in, relationships, Seq(source -> startNode, target -> endNode), InnerJoin)
            val incoming = planJoin(in, relationships, Seq(target -> startNode, source -> endNode), InnerJoin)
            relational.TabularUnionAll(outgoing, incoming)
        }

      case logical.BoundedVarLengthExpand(source, list, target, edgeScanType, direction, lower, upper, sourceOp, targetOp, _) =>

        val edgeScan = Var(list.name)(edgeScanType)
        val edgeScanOp = relational.RelationshipScan(planStart(input.graph), edgeScan)

        val isExpandInto = sourceOp == targetOp

        val planner = direction match {
          case Directed => new DirectedVarLengthExpandPlanner[T](
            source, list, edgeScan, target,
            lower, upper,
            sourceOp, edgeScanOp, targetOp,
            isExpandInto)

          case Undirected => new UndirectedVarLengthExpandPlanner[T](
            source, list, edgeScan, target,
            lower, upper,
            sourceOp, edgeScanOp, targetOp,
            isExpandInto)
        }

        planner.plan

      case logical.Optional(lhs, rhs, _) => planOptional(lhs, rhs)

      case logical.ExistsSubQuery(predicateField, lhs, rhs, _) =>

        val leftResult = process[T](lhs)
        val rightResult = process[T](rhs)

        val leftHeader = leftResult.header
        val rightHeader = rightResult.header

        // 0. Find common expressions, i.e. join expressions
        val joinExprs = leftHeader.vars.intersect(rightHeader.vars)
        // 1. Alias join expressions on rhs
        val renameExprs = joinExprs.map(e => e as Var(s"${e.name}${System.nanoTime}")(e.cypherType))
        val rightWithAliases = relational.Alias(rightResult, renameExprs.toSeq)
        // 2. Drop Join expressions and their children in rhs
        val epxrsToRemove = joinExprs.flatMap(v => rightHeader.ownedBy(v))
        val reducedRhsData = relational.Drop(rightWithAliases, epxrsToRemove)
        // 3. Compute distinct rows in rhs
        val distinctRhsData = relational.Distinct(reducedRhsData, renameExprs.map(_.alias))
        // 4. Join lhs and prepared rhs using a left outer join
        val joinedData = planJoin(leftResult, distinctRhsData, renameExprs.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)
        // 5. If at least one rhs join column is not null, the sub-query exists and true is projected to the target expression
        val targetExpr = renameExprs.head.alias
        relational.AddInto(joinedData, IsNotNull(targetExpr)(CTBoolean), predicateField)

      case logical.OrderBy(sortItems: Seq[SortItem[Expr]], in, _) =>
        relational.OrderBy(process[T](in), sortItems)

      case logical.Skip(expr, in, _) => relational.Skip(process[T](in), expr)

      case logical.Limit(expr, in, _) => relational.Limit(process[T](in), expr)

      case logical.ReturnGraph(in, _) => relational.ReturnGraph(process[T](in))

      case other => throw NotImplementedException(s"Physical planning of operator $other")
    }
  }

//    private def planConstructGraph[T <: FlatRelationalTable[T]](in: Option[LogicalOperator], construct: LogicalPatternGraph)
//      (implicit plannerContext: RelationalPlannerContext[T]): RelationalOperator[T] = {
//      val onGraphPlan = {
//        construct.onGraphs match {
//          case Nil => relational.Start() // Empty start
//          //TODO: Optimize case where no union is necessary
//          //case h :: Nil => relational.Start(Some(h)) // Just one graph, no union required
//          case several =>
//            val onGraphPlans = several.map(qgn => relational.Start(Some(qgn)))
//            relational.GraphUnionAll(onGraphPlans, construct.name)
//        }
//      }
//      val inputTablePlan = in.map(process).getOrElse(relational.Start())
//
//      val constructGraphPlan = relational.ConstructGraph(inputTablePlan, onGraphPlan, construct)
//      plannerContext.constructedGraphPlans.update(construct.name, constructGraphPlan)
//      constructGraphPlan
//      ???
//    }
  
  def planJoin[T <: FlatRelationalTable[T]](lhs: RelationalOperator[T], rhs: RelationalOperator[T], joinExprs: Seq[(Expr, Expr)], joinType: JoinType): RelationalOperator[T] = {
    
    val joinHeader = lhs.header join rhs.header
    val leftColumns = lhs.header.columns
    val rightColumns = rhs.header.columns
    
    val conflictFreeRhs = if (leftColumns ++ rightColumns != joinHeader.columns) {
      val renameColumns = rhs.header.expressions
        .filter(expr => rhs.header.column(expr) != joinHeader.column(expr))
        .map { expr => expr -> joinHeader.column(expr) }.toSeq
      
      RenameColumns(rhs, renameColumns.toMap)
    } else {
      rhs
    }

    relational.Join(lhs, conflictFreeRhs, joinExprs, joinType)
  }

  private def planStart[T <: FlatRelationalTable[T]](graph: LogicalGraph)(
    implicit plannerContext: RelationalPlannerContext[T], runtimeContext: RelationalRuntimeContext[T]): RelationalOperator[T] = {
    graph match {
      case g: LogicalCatalogGraph =>
        relational.Start(g.qualifiedGraphName, Some(plannerContext.inputRecords))(runtimeContext)
      case p: LogicalPatternGraph =>
        plannerContext.constructedGraphPlans.get(p.name) match {
          case Some(plan) => plan // the graph was already constructed
          // TODO: investigate why the implicit plannerContext is not found in scope
          case None => ??? //planConstructGraph(plannerContext.session.emptyGraphQgn, p)(plannerContext, planner) // plan starts with a construct graph, thus we have to plan it
        }
    }
  }

  // TODO: process operator outside of def
  private def planOptional[T <: FlatRelationalTable[T]](lhs: LogicalOperator, rhs: LogicalOperator)
    (implicit plannerContext: RelationalPlannerContext[T], runtimeContext: RelationalRuntimeContext[T]): RelationalOperator[T] = {
    val lhsOp = process[T](lhs)
    val rhsOp = process[T](rhs)

    val lhsHeader = lhsOp.header
    val rhsHeader = rhsOp.header

    def generateUniqueName = s"tmp${System.nanoTime}"

    // 1. Compute expressions between left and right side
    val commonExpressions = lhsHeader.expressions.intersect(rhsHeader.expressions)
    val joinExprs = commonExpressions.collect { case v: Var => v }
    val otherExpressions = commonExpressions -- joinExprs

    // 2. Remove siblings of the join expressions and other common fields
    val expressionsToRemove = joinExprs
      .flatMap(v => rhsHeader.ownedBy(v) - v)
      .union(otherExpressions)
    val rhsWithDropped = relational.Drop(rhsOp, expressionsToRemove)

    // 3. Rename the join expressions on the right hand side, in order to make them distinguishable after the join
    val joinExprRenames = joinExprs.map(e => e as Var(generateUniqueName)(e.cypherType))
    val rhsWithAlias = relational.Alias(rhsWithDropped, joinExprRenames.toSeq)
    val rhsJoinReady = relational.Drop(rhsWithAlias, joinExprs.collect { case e: Expr => e })

    // 4. Left outer join the left side and the processed right side
    val joined = planJoin(lhsOp, rhsJoinReady, joinExprRenames.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)

    // 5. Select the resulting header expressions
    relational.Select(joined, joined.header.expressions.toList)
  }
}
