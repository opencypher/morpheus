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
package org.opencypher.okapi.relational.impl.planning

import org.opencypher.okapi.api.graph.CypherSession
import org.opencypher.okapi.api.types.{CTBoolean, CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.{NotImplementedException, SchemaException}
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.logical.{impl => logical}
import org.opencypher.okapi.relational.api.planning.{RelationalPlannerContext, RelationalRuntimeContext}
import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.operators._
import org.opencypher.okapi.relational.impl.planning.ConstructGraphPlanner._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.relational.impl.{operators => relational}
import org.opencypher.okapi.relational.api.tagging.Tags._

object RelationalPlanner {

  // TODO: rename to 'plan'
  def process[T <: Table[T]](input: LogicalOperator)(
    // TODO: unify contexts?
    implicit plannerContext: RelationalPlannerContext[T],
    runtimeContext: RelationalRuntimeContext[T]
  ): RelationalOperator[T] = {

    implicit val caps: CypherSession = plannerContext.session

    input match {
      case logical.CartesianProduct(lhs, rhs, _) =>
        planJoin(process[T](lhs), process[T](rhs), Seq.empty, CrossJoin)

      case logical.Select(fields, in, _) =>

        val inOp = process[T](in)

        val selectExpressions = fields
          .flatMap(inOp.header.ownedBy)
          .distinct

        relational.Select(inOp, selectExpressions)

      case logical.Project(projectExpr, in, _) =>
        val inOp = process[T](in)
        val (expr, maybeAlias) = projectExpr
        val containsExpr = inOp.header.contains(expr)

        maybeAlias match {
          case Some(alias) if containsExpr => relational.Alias(inOp, Seq(expr as alias))
          case Some(alias) => relational.Project(inOp, expr as alias)
          case None => relational.Project(inOp, expr)
        }

      case logical.EmptyRecords(fields, in, _) =>
        relational.EmptyRecords(process[T](in), fields)

      case logical.Start(graph, _) => planStart(graph)

      case logical.FromGraph(graph, in, _) =>
        graph match {
          case g: LogicalCatalogGraph =>
            relational.FromGraph(process[T](in), g)

          case construct: LogicalPatternGraph =>
            planConstructGraph(Some(in), construct) //(plannerContext, runtimeContext)
        }

      case logical.Unwind(list, item, in, _) =>
        val explodeExpr = Explode(list)(item.cypherType)
        relational.Project(process[T](in), explodeExpr as item)

      case logical.NodeScan(v, in, _) => relational.Scan(process[T](in), v.cypherType).assignScanName(v.name)

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

        val second = relational.Scan(startFrom, rel.cypherType).assignScanName(rel.name)
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

            val tempIncoming = planJoin(first, relsWithoutLoops, Seq(source -> endNode), InnerJoin)
            val incoming = planJoin(tempIncoming, third, Seq(startNode -> target), InnerJoin)

            relational.TabularUnionAll(outgoing, incoming)
        }

      case logical.ExpandInto(source, rel, target, direction, sourceOp, _) =>
        val in = process[T](sourceOp)
        val relationships = relational.Scan(in, rel.cypherType).assignScanName(rel.name)

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
        val edgeScanOp = relational.Scan(planStart(input.graph), edgeScan.cypherType).assignScanName(edgeScan.name)

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
        val exprsToRemove = joinExprs.flatMap(v => rightHeader.ownedBy(v))
        val reducedRhsData = relational.Drop(rightWithAliases, exprsToRemove)
        // 3. Compute distinct rows in rhs
        val distinctRhsData = relational.Distinct(reducedRhsData, renameExprs.map(_.alias))
        // 4. Join lhs and prepared rhs using a left outer join
        val joinedData = planJoin(leftResult, distinctRhsData, renameExprs.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)
        // 5. If at least one rhs join column is not null, the sub-query exists and true is projected to the target expression
        val targetExpr = renameExprs.head.alias
        relational.ProjectInto(joinedData, IsNotNull(targetExpr)(CTBoolean), predicateField.targetField)

      case logical.OrderBy(sortItems: Seq[SortItem[Expr]], in, _) =>
        relational.OrderBy(process[T](in), sortItems)

      case logical.Skip(expr, in, _) => relational.Skip(process[T](in), expr)

      case logical.Limit(expr, in, _) => relational.Limit(process[T](in), expr)

      case logical.ReturnGraph(in, _) => relational.ReturnGraph(process[T](in))

      case other => throw NotImplementedException(s"Physical planning of operator $other")
    }
  }

  def planJoin[T <: Table[T]](
    lhs: RelationalOperator[T],
    rhs: RelationalOperator[T],
    joinExprs: Seq[(Expr, Expr)],
    joinType: JoinType
  ): RelationalOperator[T] = {
    val joinHeader = lhs.header join rhs.header
    val conflictFreeRhs = rhs.alignColumnNames(joinHeader)
    relational.Join(lhs, conflictFreeRhs, joinExprs, joinType)
  }

  private def planStart[T <: Table[T]](graph: LogicalGraph)(
    implicit plannerContext: RelationalPlannerContext[T], runtimeContext: RelationalRuntimeContext[T]
  ): RelationalOperator[T] = {
    graph match {
      case g: LogicalCatalogGraph =>
        relational.Start(g.qualifiedGraphName, Some(plannerContext.inputRecords))(runtimeContext)
      case p: LogicalPatternGraph =>
        plannerContext.constructedGraphPlans.get(p.name) match {
          case Some(plan) => plan // the graph was already constructed
          case None => planConstructGraph(None, p)
        }
    }
  }

  // TODO: process operator outside of def
  private def planOptional[T <: Table[T]](lhs: LogicalOperator, rhs: LogicalOperator)
    (
      implicit plannerContext: RelationalPlannerContext[T],
      runtimeContext: RelationalRuntimeContext[T]
    ): RelationalOperator[T] = {
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

  implicit class RelationalOperatorOps[T <: Table[T]](val op: RelationalOperator[T]) extends AnyVal {

    def retagVariable(v: Var, replacements: Map[Int, Int]): RelationalOperator[T] = {
      op.header.idExpressions(v).foldLeft(op) {
        case (currentOp, exprToRetag) =>
          currentOp.projectInto(exprToRetag.replaceTags(replacements), exprToRetag)
      }
    }

    def drop(exprs: Expr*): RelationalOperator[T] = {
      relational.Drop(op, exprs.toSet)
    }

    def project(value: Expr): RelationalOperator[T] = {
      relational.Project(op, value)
    }

    def projectInto(value: Expr, into: Expr): RelationalOperator[T] = {
      relational.ProjectInto(op, value, into)
    }

    // Only works with single entity tables
    def assignScanName(name: String): RelationalOperator[T] = {
      val scanVar = op.singleEntity
      val namedScanVar = Var(name)(scanVar.cypherType)
      Drop(Alias(op, Seq(scanVar as namedScanVar)), Set(scanVar))
    }

    def alignWith(entity: Var, targetHeader: RecordHeader): RelationalOperator[T] = {
      op.alignExpressions(entity, targetHeader).alignColumnNames(targetHeader)
    }

    // TODO: entity needs to contain all labels/relTypes: all case needs to be explicitly expanded with the schema
    def alignExpressions(targetVar: Var, targetHeader: RecordHeader): RelationalOperator[T] = {

      val inputVar = op.singleEntity
      val targetHeaderLabels = targetHeader.labelsFor(targetVar).map(_.label.name)
      val targetHeaderTypes = targetHeader.typesFor(targetVar).map(_.relType.name)

      // Labels/RelTypes that do not need to be added
      val existingLabels = op.header.labelsFor(inputVar).map(_.label.name)
      val existingRelTypes = op.header.typesFor(inputVar).map(_.relType.name)

      // Rename variable and select columns owned by entityVar
      val renamedEntity = relational.Select(op, List(inputVar as targetVar))

      // Drop expressions that are not in the target header
      val dropExpressions = renamedEntity.header.expressions -- targetHeader.expressions
      val withDroppedExpressions = if (dropExpressions.nonEmpty) relational.Drop(renamedEntity, dropExpressions) else renamedEntity

      // Fill in missing true label columns
      val trueLabels = inputVar.cypherType match {
        case CTNode(labels, _) => (targetHeaderLabels intersect labels) -- existingLabels
        case _ => Set.empty
      }
      val withTrueLabels = trueLabels.foldLeft(withDroppedExpressions: RelationalOperator[T]) {
        case (currentOp, label) => relational.ProjectInto(currentOp, TrueLit, HasLabel(targetVar, Label(label))(CTBoolean))
      }

      // Fill in missing false label columns
      val falseLabels = targetVar.cypherType match {
        case _: CTNode => targetHeaderLabels -- trueLabels -- existingLabels
        case _ => Set.empty
      }
      val withFalseLabels = falseLabels.foldLeft(withTrueLabels: RelationalOperator[T]) {
        case (currentOp, label) => relational.ProjectInto(currentOp, FalseLit, HasLabel(targetVar, Label(label))(CTBoolean))
      }

      // Fill in missing true relType columns
      val trueRelTypes = inputVar.cypherType match {
        case CTRelationship(relTypes, _) => (targetHeaderTypes intersect relTypes) -- existingRelTypes
        case _ => Set.empty
      }
      val withTrueRelTypes = trueRelTypes.foldLeft(withFalseLabels: RelationalOperator[T]) {
        case (currentOp, relType) => relational.ProjectInto(currentOp, TrueLit, HasType(targetVar, RelType(relType))(CTBoolean))
      }

      // Fill in missing false relType columns
      val falseRelTypes = targetVar.cypherType match {
        case _: CTRelationship => targetHeaderTypes -- trueRelTypes -- existingRelTypes
        case _ => Set.empty
      }
      val withFalseRelTypes = falseRelTypes.foldLeft(withTrueRelTypes: RelationalOperator[T]) {
        case (currentOp, relType) => relational.ProjectInto(currentOp, FalseLit, HasType(targetVar, RelType(relType))(CTBoolean))
      }

      // Fill in missing properties
      val missingProperties = targetHeader.propertiesFor(targetVar) -- withFalseRelTypes.header.propertiesFor(targetVar)
      val withProperties = missingProperties.foldLeft(withFalseRelTypes: RelationalOperator[T]) {
        case (currentOp, propertyExpr) => relational.ProjectInto(currentOp, NullLit(propertyExpr.cypherType), propertyExpr)
      }

      import Expr._
      assert(targetHeader.expressions == withProperties.header.expressions,
        s"Expected header expressions:\n\t${targetHeader.expressions.toSeq.sorted.mkString(", ")},\ngot\n\t${withProperties.header.expressions.toSeq.sorted.mkString(", ")}")
      withProperties
    }

    def alignColumnNames(targetHeader: RecordHeader): RelationalOperator[T] = {
      val exprsNotInTarget = op.header.expressions -- targetHeader.expressions
      require(exprsNotInTarget.isEmpty,
        s"""|Column alignment requires for all header expressions to be present in the target header:
            |Current: ${op.header}
            |Target: $targetHeader
            |Missing expressions: ${exprsNotInTarget.mkString(", ")}
        """.stripMargin)

      if (op.header.expressions.forall(expr => op.header.column(expr) == targetHeader.column(expr))) {
        op
      } else {
        val columnRenamings = op.header.expressions.foldLeft(Map.empty[String, String]) {
          case (currentMap, expr) => currentMap + (op.header.column(expr) -> targetHeader.column(expr))
        }
        RenameColumns(op, columnRenamings)
      }
    }

    def singleEntity: Var = {
      op.header.entityVars.toList match {
        case entity :: Nil => entity
        case Nil => throw SchemaException(s"Operation requires single entity table, input contains no entities")
        case other => throw SchemaException(s"Operation requires single entity table, found ${other.mkString("[", ", ", "]")}")
      }
    }

    def filterRelTypes(targetType: CTRelationship): RelationalOperator[T] = {
      val singleEntity = op.singleEntity

      if (singleEntity.cypherType.subTypeOf(targetType).isTrue) {
        op
      } else {
        val relTypes = op.header
          .typesFor(singleEntity)
          .filter(hasType => targetType.types.contains(hasType.relType.name))
        val filterExpr = Ors(relTypes.map(Equals(_, TrueLit)(CTBoolean)).toSeq: _*)
        relational.Filter(op, filterExpr)
      }
    }
  }

}
