/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{NotImplementedException, SchemaException, UnsupportedOperationException}
import org.opencypher.okapi.impl.types.CypherTypeUtils._
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.ir.impl.util.VarConverters._
import org.opencypher.okapi.logical.impl.{Incoming, Outgoing, _}
import org.opencypher.okapi.logical.{impl => logical}
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.operators._
import org.opencypher.okapi.relational.impl.planning.ConstructGraphPlanner._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.relational.impl.{operators => relational}

import scala.reflect.runtime.universe.TypeTag

object RelationalPlanner {

  // TODO: rename to 'plan'
  def process[T <: Table[T] : TypeTag](input: LogicalOperator)
    (implicit context: RelationalRuntimeContext[T]): RelationalOperator[T] = {

    implicit val caps: CypherSession = context.session

    input match {
      case logical.CartesianProduct(lhs, rhs, _) =>
        process[T](lhs).join(process[T](rhs), Seq.empty, CrossJoin)

      case logical.Select(fields, in, _) =>

        val inOp = process[T](in)

        val selectExpressions = fields
          .flatMap(inOp.header.ownedBy)
          .distinct

        inOp.select(selectExpressions: _*)

      case logical.Project(projectExpr, in, _) =>
        val inOp = process[T](in)
        val (expr, maybeAlias) = projectExpr
        val containsExpr = inOp.header.contains(expr)

        maybeAlias match {
          case Some(alias) if containsExpr => inOp.alias(expr as alias)
          case Some(alias) => inOp.add(expr as alias)
          case None => inOp.add(expr)
        }

      case logical.EmptyRecords(fields, in, _) =>
        relational.EmptyRecords(process[T](in), fields)

      case logical.Start(graph, _) => relational.Start(graph.qualifiedGraphName)

      case logical.DrivingTable(graph, _, _) => relational.Start(graph.qualifiedGraphName, context.maybeInputRecords)

      case logical.FromGraph(graph, in, _) =>
        val inOp = process[T](in)
        graph match {
          case g: LogicalCatalogGraph => relational.FromCatalogGraph(inOp, g)
          case construct: LogicalPatternGraph => planConstructGraph(inOp, construct)
        }

      case logical.Unwind(list, item, in, _) =>
        val explodeExpr = Explode(list)
        process[T](in).add(explodeExpr as item)

      case logical.PatternScan(pattern, mapping, in, _) =>
        planScan(
          Some(process[T](in)),
          in.graph,
          pattern,
          mapping
        )

      case logical.Aggregate(aggregations, group, in, _) => relational.Aggregate(process[T](in), group, aggregations)

      case logical.Filter(expr, in, _) => process[T](in).filter(expr)

      case logical.ValueJoin(lhs, rhs, predicates, _) =>
        val joinExpressions = predicates.map(p => p.lhs -> p.rhs).toSeq
        process[T](lhs).join(process[T](rhs), joinExpressions, InnerJoin)

      case logical.Distinct(fields, in, _) =>
        val entityExprs: Set[Var] = Set(fields.toSeq: _*)
        relational.Distinct(process[T](in), entityExprs)

      case logical.TabularUnionAll(left, right) =>
        process[T](left).unionAll(process[T](right))

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case logical.Expand(source, rel, target, direction, sourceOp, targetOp, _) =>
        val first = process[T](sourceOp)
        val third = process[T](targetOp)

        val relPattern = RelationshipPattern(rel.cypherType.toCTRelationship)
        val second = planScan(
          None,
          sourceOp.graph,
          relPattern,
          Map(rel -> relPattern.relEntity)
        )

        val startNode = StartNode(rel)(CTNode)
        val endNode = EndNode(rel)(CTNode)

        direction match {
          case Outgoing =>
            val tempResult = first.join(second, Seq(source -> startNode), InnerJoin)
            tempResult.join(third, Seq(endNode -> target), InnerJoin)

          case org.opencypher.okapi.logical.impl.Incoming =>
            val tempResult = third.join(second, Seq(target -> endNode), InnerJoin)
            tempResult.join(first, Seq(startNode -> source), InnerJoin)

          case Undirected =>
            val tempOutgoing = first.join(second, Seq(source -> startNode), InnerJoin)
            val outgoing = tempOutgoing.join(third, Seq(endNode -> target), InnerJoin)

            val filterExpression = Not(Equals(startNode, endNode))
            val relsWithoutLoops = second.filter(filterExpression)

            val tempIncoming = first.join(relsWithoutLoops, Seq(source -> endNode), InnerJoin)
            val incoming = tempIncoming.join(third, Seq(startNode -> target), InnerJoin)

            relational.TabularUnionAll(outgoing, incoming)
        }

      case logical.ExpandInto(source, rel, target, direction, sourceOp, _) =>
        val in = process[T](sourceOp)

        val relPattern = RelationshipPattern(rel.cypherType.toCTRelationship)
        val relationships = planScan(
          None,
          sourceOp.graph,
          relPattern,
          Map(rel -> relPattern.relEntity)
        )

        val startNode = StartNode(rel)(CTAny)
        val endNode = EndNode(rel)(CTAny)

        direction match {
          case Outgoing | Incoming =>
            in.join(relationships, Seq(source -> startNode, target -> endNode), InnerJoin)

          case Undirected =>
            val outgoing = in.join(relationships, Seq(source -> startNode, target -> endNode), InnerJoin)
            val incoming = in.join(relationships, Seq(target -> startNode, source -> endNode), InnerJoin)
            relational.TabularUnionAll(outgoing, incoming)
        }

      case logical.BoundedVarLengthExpand(source, list, target, edgeScanType, direction, lower, upper, sourceOp, targetOp, _) =>

        val edgeScan = Var(list.name)(edgeScanType)

        val edgePattern = RelationshipPattern(edgeScanType)
        val edgeScanOp = planScan(
          None,
          sourceOp.graph,
          edgePattern,
          Map(edgeScan -> edgePattern.relEntity)
        )

        val isExpandInto = sourceOp == targetOp

        val planner = direction match {
          // TODO: verify that var length is able to traverse in different directions
          case Outgoing | Incoming => new DirectedVarLengthExpandPlanner[T](
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
        val rightWithAliases = rightResult.alias(renameExprs)
        // 2. Drop Join expressions and their children in rhs
        val exprsToRemove = joinExprs.flatMap(v => rightHeader.ownedBy(v))
        val reducedRhsData = rightWithAliases.dropExprSet(exprsToRemove)
        // 3. Compute distinct rows in rhs
        val distinctRhsData = relational.Distinct(reducedRhsData, renameExprs.map(_.alias))
        // 4. Join lhs and prepared rhs using a left outer join
        val joinedData = leftResult.join(distinctRhsData, renameExprs.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)
        // 5. If at least one rhs join column is not null, the sub-query exists and true is projected to the target expression
        val targetExpr = renameExprs.head.alias
        joinedData.addInto(IsNotNull(targetExpr) -> predicateField.targetField)

      case logical.OrderBy(sortItems: Seq[SortItem], in, _) =>
        relational.OrderBy(process[T](in), sortItems)

      case logical.Skip(expr, in, _) =>
        relational.Skip(process[T](in), expr)

      case logical.Limit(expr, in, _) =>
        relational.Limit(process[T](in), expr)

      case logical.ReturnGraph(in, _) => relational.ReturnGraph(process[T](in))

      case other => throw NotImplementedException(s"Physical planning of operator $other")
    }
  }

  def planScan[T <: Table[T] : TypeTag](
    maybeInOp: Option[RelationalOperator[T]],
    logicalGraph: LogicalGraph,
    scanPattern: Pattern,
    varPatternEntityMapping: Map[Var, Entity]
  )(implicit context: RelationalRuntimeContext[T]): RelationalOperator[T] = {
    val inOp = maybeInOp match {
      case Some(relationalOp) => relationalOp
      case _ => relational.Start(logicalGraph.qualifiedGraphName)
    }

    val graph = logicalGraph match {
      case _: LogicalCatalogGraph =>
        inOp.context.resolveGraph(logicalGraph.qualifiedGraphName)
      case p: LogicalPatternGraph =>
        inOp.context.queryLocalCatalog.getOrElse(p.qualifiedGraphName, planConstructGraph(inOp, p).graph)
    }

    val scanOp = graph.scanOperator(scanPattern)

    val validScan = scanPattern.entities.forall { entity =>
      scanOp.header.entityVars.exists { headerVar =>
        headerVar.name == entity.name && headerVar.cypherType.withoutGraph == entity.cypherType.withoutGraph
      }
    }

    if (!validScan) throw SchemaException(s"Expected the scan to include Variables for all entities of ${scanPattern.entities}" +
      s" but got ${scanOp.header.entityVars}")

    scanOp
      .assignScanName(varPatternEntityMapping.mapValues(_.toVar).map(_.swap))
      .switchContext(inOp.context)
  }

  // TODO: process operator outside of def
  private def planOptional[T <: Table[T] : TypeTag](lhs: LogicalOperator, rhs: LogicalOperator)
    (implicit context: RelationalRuntimeContext[T]): RelationalOperator[T] = {
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
    val joined = lhsOp.join(rhsJoinReady, joinExprRenames.map(a => a.expr -> a.alias).toSeq, LeftOuterJoin)

    // 5. Select the resulting header expressions
    relational.Select(joined, joined.header.expressions.toList)
  }

  implicit class RelationalOperatorOps[T <: Table[T] : TypeTag](op: RelationalOperator[T]) {
    private implicit def context: RelationalRuntimeContext[T] = op.context

    def select(expressions: Expr*): RelationalOperator[T] = relational.Select(op, expressions.toList)

    def filter(expression: Expr): RelationalOperator[T] = {
      if (expression == TrueLit) {
        op
      } else if (expression.cypherType == CTNull) {
        relational.Start.fromEmptyGraph(context.session.records.empty(op.header))
      } else {
        relational.Filter(op, expression)
      }
    }

    /**
      * Renames physical columns to given header expression names.
      * Ensures that there is a physical column for each return item, i.e. aliases lead to duplicate physical columns.
      */
    def alignColumnsWithReturnItems: RelationalOperator[T] = {
      val selectExprs = op.maybeReturnItems.getOrElse(List.empty)
        .flatMap(op.header.expressionsFor)
        .toList

      val renames = selectExprs
        .map(expr => expr -> expr.withoutType.toString.replace('.', '_'))
        .toMap

      relational.Select(op, selectExprs, renames)
    }

    def renameColumns(columnRenames: Map[Expr, String]): RelationalOperator[T] = {
      if (columnRenames.isEmpty) op else relational.Select(op, op.header.expressions.toList, columnRenames)
    }

    def join(other: RelationalOperator[T], joinExprs: Seq[(Expr, Expr)], joinType: JoinType): RelationalOperator[T] = {
      relational.Join(op, other.withDisjointColumnNames(op.header), joinExprs, joinType)
    }

    def unionAll(other: RelationalOperator[T]): RelationalOperator[T] = {
      val combinedHeader = op.header union other.header

      // rename all columns to make sure we have no conflicts
      val targetHeader = RecordHeader.empty.withExprs(combinedHeader.expressions)

      val elementVars = targetHeader.nodeVars ++ targetHeader.relationshipVars

      val opWithAlignedEntities = elementVars.foldLeft(op) {
        case (acc, elementVar) => acc.alignExpressions(elementVar, elementVar, targetHeader)
      }.alignColumnNames(targetHeader)

      val otherWithAlignedEntities = elementVars.foldLeft(other) {
        case (acc, elementVar) => acc.alignExpressions(elementVar, elementVar, targetHeader)
      }.alignColumnNames(targetHeader)

      relational.TabularUnionAll(opWithAlignedEntities, otherWithAlignedEntities)
    }

    def add(values: Expr*): RelationalOperator[T] = {
      if (values.isEmpty) op else relational.Add(op, values.toList)
    }

    def addInto(valueIntos: (Expr, Expr)*): RelationalOperator[T] = {
      if (valueIntos.isEmpty) op else relational.AddInto(op, valueIntos.toList)
    }

    def dropExprSet[E <: Expr](expressions: Set[E]): RelationalOperator[T] = {
      val necessaryDrops = expressions.filter(op.header.expressions.contains)
      if (necessaryDrops.nonEmpty) {
        relational.Drop(op, necessaryDrops)
      } else op
    }

    def dropExpressions[E <: Expr](expressions: E*): RelationalOperator[T] = {
      dropExprSet(expressions.toSet)
    }

    def alias(aliases: AliasExpr*): RelationalOperator[T] = Alias(op, aliases)

    def alias(aliases: Set[AliasExpr]): RelationalOperator[T] = alias(aliases.toSeq: _*)

    // Only works with single entity tables
    def assignScanName(mapping: Map[Var, Var]): RelationalOperator[T] = {
      val aliases = mapping.map {
        case (from, to) => AliasExpr(from, to)
      }

      op.select(aliases.toList: _*)
    }

    def switchContext(context: RelationalRuntimeContext[T]): RelationalOperator[T] = {
      SwitchContext(op, context)
    }

    def prefixVariableId(v: Var, prefix: GraphIdPrefix): RelationalOperator[T] = {
      val prefixedIds = op.header.idExpressions(v).map(exprToPrefix => PrefixId(ToId(exprToPrefix), prefix) -> exprToPrefix)
      op.addInto(prefixedIds.toSeq: _*)
    }

    def alignWith(inputEntity: Var, targetEntity: Var, targetHeader: RecordHeader): RelationalOperator[T] = {
      op.alignExpressions(inputEntity, targetEntity, targetHeader).alignColumnNames(targetHeader)
    }

    // TODO: entity needs to contain all labels/relTypes: all case needs to be explicitly expanded with the schema
    /**
      * Aligns a single element within the operator with the given target entity in the target header.
      *
      * @param inputVar     the variable of the element that should be aligned
      * @param targetVar    the variable of the reference element
      * @param targetHeader the header describing the desired state
      * @return operator with aligned element
      */
    def alignExpressions(inputVar: Var, targetVar: Var, targetHeader: RecordHeader): RelationalOperator[T] = {

      val targetHeaderLabels = targetHeader.labelsFor(targetVar).map(_.label.name)
      val targetHeaderTypes = targetHeader.typesFor(targetVar).map(_.relType.name)

      // Labels/RelTypes that do not need to be added
      val existingLabels = op.header.labelsFor(inputVar).map(_.label.name)
      val existingRelTypes = op.header.typesFor(inputVar).map(_.relType.name)

      val otherEntities = op.header -- Set(inputVar)
      val toRetain = otherEntities.expressions + (inputVar as targetVar)

      // Rename variable and select columns owned by entityVar
      val renamedEntity = op.select(toRetain.toSeq: _*)

      // Drop expressions that are not in the target header
      val dropExpressions = renamedEntity.header.expressions -- targetHeader.expressions
      val withDroppedExpressions = renamedEntity.dropExprSet(dropExpressions)

      // Fill in missing true label columns
      val trueLabels = inputVar.cypherType match {
        case CTNode(labels, _) => (targetHeaderLabels intersect labels) -- existingLabels
        case _ => Set.empty
      }
      val withTrueLabels = withDroppedExpressions.addInto(
        trueLabels.map(label => TrueLit -> HasLabel(targetVar, Label(label))).toSeq: _*
      )

      // Fill in missing false label columns
      val falseLabels = targetVar.cypherType match {
        case n if n.subTypeOf(CTNode.nullable) => targetHeaderLabels -- trueLabels -- existingLabels
        case _ => Set.empty
      }
      val withFalseLabels = withTrueLabels.addInto(
        falseLabels.map(label => FalseLit -> HasLabel(targetVar, Label(label))).toSeq: _*
      )

      // Fill in missing true relType columns
      val trueRelTypes = inputVar.cypherType match {
        case CTRelationship(relTypes, _) => (targetHeaderTypes intersect relTypes) -- existingRelTypes
        case _ => Set.empty
      }
      val withTrueRelTypes = withFalseLabels.addInto(
        trueRelTypes.map(relType => TrueLit -> HasType(targetVar, RelType(relType))).toSeq: _*
      )

      // Fill in missing false relType columns
      val falseRelTypes = targetVar.cypherType match {
        case r if r.subTypeOf(CTRelationship.nullable) => targetHeaderTypes -- trueRelTypes -- existingRelTypes
        case _ => Set.empty
      }
      val withFalseRelTypes = withTrueRelTypes.addInto(
        falseRelTypes.map(relType => FalseLit -> HasType(targetVar, RelType(relType))).toSeq: _*
      )

      // Fill in missing properties
      val missingProperties = targetHeader.propertiesFor(targetVar) -- withFalseRelTypes.header.propertiesFor(targetVar)
      val withProperties = withFalseRelTypes.addInto(
        missingProperties.map(propertyExpr => NullLit -> propertyExpr).toSeq: _*
      )

      import Expr._
      assert(targetHeader.expressionsFor(targetVar) == withProperties.header.expressionsFor(targetVar),
        s"""Expected header expressions for $targetVar:
           |\t${targetHeader.expressionsFor(targetVar).toSeq.sorted.mkString(", ")},
           |got
           |\t${withProperties.header.expressionsFor(targetVar).toSeq.sorted.mkString(", ")}""".stripMargin)
      withProperties
    }

    /**
      * Returns an operator with renamed columns such that the operators columns do not overlap with the other header's
      * columns.
      *
      * @param otherHeader header from which the column names should be disjoint
      * @return operator with disjoint column names
      */
    def withDisjointColumnNames(otherHeader: RecordHeader): RelationalOperator[T] = {
      val header = op.header
      val conflictingExpressions = header.expressions.filter(e => otherHeader.columns.contains(header.column(e)))

      if (conflictingExpressions.isEmpty) {
        op
      } else {
        val renameMapping = conflictingExpressions.foldLeft(Map.empty[Expr, String]) {
          case (acc, nextRename) =>
            val newColumnName = header.newConflictFreeColumnName(nextRename, otherHeader.columns ++ acc.values)
            acc + (nextRename -> newColumnName)
        }
        op.renameColumns(renameMapping)
      }
    }

    /**
      * Ensures that the column names are aligned with the target header.
      *
      * @note All expressions in the operator header must be present in the target header.
      * @param targetHeader the header with which the column names should be aligned with
      * @return operator with aligned column names
      */
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
        val columnRenames = op.header.expressions.foldLeft(Map.empty[Expr, String]) {
          case (currentMap, expr) => currentMap + (expr -> targetHeader.column(expr))
        }
        op.renameColumns(columnRenames)
      }
    }

    def singleEntity: Var = {
      op.header.entityVars.toList match {
        case entity :: Nil => entity
        case Nil => throw SchemaException(s"Operation requires single entity table, input contains no entities")
        case other => throw SchemaException(s"Operation requires single entity table, found ${other.mkString("[", ", ", "]")}")
      }
    }

    def entityTable: EntityTable[T] = {
      val entity = op.singleEntity

      val header = op.header
      val idCol = header.idColumns(entity).head
      val properties = header.propertiesFor(entity).map(p => p -> header.column(p))
      val propertyMapping = properties.map { case (p: Property, column) => p.key.name -> column }

      entity.cypherType match {
        case CTNode(labels, _) =>
          val mapping = NodeMappingBuilder
            .on(idCol)
            .withImpliedLabels(labels.toSeq: _*)
            .withPropertyKeyMappings(propertyMapping.toSeq: _*)
            .build

          op.session.entityTables.entityTable(mapping, op.table)

        case CTRelationship(typ, _) =>
          val sourceCol = header.column(header.startNodeFor(entity))
          val targetCol = header.column(header.endNodeFor(entity))
          val mapping = RelationshipMappingBuilder
            .on(idCol)
            .from(sourceCol)
            .to(targetCol)
            .withRelType(typ.head)
            .withPropertyKeyMappings(propertyMapping.toSeq: _*)
            .build

          op.session.entityTables.entityTable(mapping, op.table)

        case other => throw UnsupportedOperationException(s"Cannot create scan for $other")
      }
    }
  }

}
