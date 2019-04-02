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
package org.opencypher.okapi.logical.impl

import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.types.{CTBoolean, CTNode, CTRelationship}
import org.opencypher.okapi.impl.types.CypherTypeUtils._
import org.opencypher.okapi.ir.api.IRField
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.trees.{BottomUp, BottomUpWithContext}

import scala.util.Try

object LogicalOptimizer extends DirectCompilationStage[LogicalOperator, LogicalOperator, LogicalPlannerContext] {

  override def process(input: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    val optimizationRules = Seq(
      discardScansForNonexistentLabels,
      replaceCartesianWithValueJoin,
      replaceScansWithRecognizedPatterns
    )
      optimizationRules.foldLeft(input) {
      // TODO: Evaluate if multiple rewriters could be fused
      case (tree: LogicalOperator, optimizationRule) => BottomUp[LogicalOperator](optimizationRule).transform(tree)
    }
  }

  def replaceCartesianWithValueJoin: PartialFunction[LogicalOperator, LogicalOperator] = {
    case filter@Filter(e @ CanOptimize(leftField, rightField), in, _) =>
      val (newChild, rewritten) = BottomUpWithContext[LogicalOperator, Boolean] {
        case (CartesianProduct(lhs, rhs, solved), false) if solved.solves(leftField) && solved.solves(rightField) =>
          val (leftExpr, rightExpr) = if (lhs.solved.solves(leftField)) e.lhs -> e.rhs else e.rhs -> e.lhs
          val joinExpr = Equals(leftExpr, rightExpr)
          val leftProject = Project(leftExpr -> None, lhs, lhs.solved)
          val rightProject = Project(rightExpr -> None, rhs, rhs.solved)
          ValueJoin(leftProject, rightProject, Set(joinExpr), solved.withPredicate(joinExpr)) -> true
      }.transform(in, context = false)

      if (rewritten) newChild else filter
  }

  def replaceScansWithRecognizedPatterns(implicit context: LogicalPlannerContext): PartialFunction[LogicalOperator, LogicalOperator] = {
    case exp: Expand =>
      exp.source.cypherType.graph.map { g =>

        val availablePatterns: Set[Pattern] =
          Try {
            context.resolveGraph(g)
          }.map { g =>
            g.patterns
              .collect {
                case nr: NodeRelPattern => nr
                case nrn: TripletPattern => nrn
              }.toList
              .sorted(Pattern.PatternOrdering)
              .reverse
          }.getOrElse(Set.empty).toSet

        if ( graphProvidesTripletPatternFor(exp, availablePatterns, g, context) ) {
          val sourceType = exp.source.cypherType.toCTNode
          val relType = exp.rel.cypherType.toCTRelationship
          val targetType = exp.target.cypherType.toCTNode

          val pattern = TripletPattern(sourceType, relType, targetType)

          val withPatternScan = replaceScans(exp.rhs, exp.target, pattern) { parent =>
            val map = Map(exp.source -> pattern.sourceEntity, exp.rel -> pattern.relEntity, exp.target -> pattern.targetEntity)
            PatternScan(pattern, map, parent, parent.solved.withFields(map.keySet.map(_.toField.get).toList:_ *))
          }
          replaceScans(exp.lhs, exp.source, pattern){_ => withPatternScan}

        } else if ( graphProvidesNodeRelPatternFor(exp, availablePatterns, g, context) ){
          val nodeType = exp.source.cypherType.toCTNode
          val relType = exp.rel.cypherType.toCTRelationship

          val pattern = NodeRelPattern(nodeType, relType)
          val withPatternScan = replaceScans(exp.lhs, exp.source, pattern) { parent =>
            val map = Map(exp.source -> pattern.nodeEntity, exp.rel -> pattern.relEntity)
            PatternScan(pattern, map, parent, parent.solved.withFields(map.keySet.map(_.toField.get).toList:_ *))
          }

          val joinExpr = Equals(EndNode(exp.rel)(CTNode), exp.target)
          ValueJoin(withPatternScan, exp.rhs, Set(joinExpr), exp.solved)

        } else {
          exp
        }
      }.getOrElse(exp)
  }

  def replaceScans(subtree: LogicalOperator, varToReplace: Var, pattern: Pattern)(f: LogicalOperator => LogicalOperator):LogicalOperator = {
   def rewriter: PartialFunction[LogicalOperator, LogicalOperator] = {
      case PatternScan(_: NodePattern, mapping, parent, _) if mapping.keySet.contains(varToReplace) => f(parent)

      case pScan: PatternScan if pScan.mapping.contains(varToReplace) =>
        val renamedVarToReplace = Var(varToReplace.name + "_renamed")(varToReplace.cypherType)

        val replaceOp = f(pScan.in)
        val withAliasedVar = Project(varToReplace -> Some(renamedVarToReplace), replaceOp, replaceOp.solved.withFields(renamedVarToReplace.toField.get))
        val toSelect = replaceOp.fields - varToReplace + renamedVarToReplace
        val selectOp = Select(toSelect.toList, withAliasedVar, replaceOp.solved.withFields(renamedVarToReplace.toField.get))

        val joinExpr = Equals(varToReplace, renamedVarToReplace)
        val joinOp = ValueJoin(pScan, selectOp, Set(joinExpr), pScan.solved ++ selectOp.solved)
        Select((pScan.mapping.keySet ++ selectOp.fields).toList, joinOp, joinOp.solved)
    }

    BottomUp[LogicalOperator](rewriter).transform(subtree)
  }

  private object CanOptimize {
    def unapply(e: Equals): Option[(IRField, IRField)] = {
      for (l <- e.lhs.toField; r <- e.rhs.toField) yield (l, r)
    }
  }

  private implicit class RichExpr(val expr: Expr) extends AnyVal {
    @scala.annotation.tailrec
    final def toField: Option[IRField] = expr match {
      case v: Var => Some(IRField(v.name)(v.cypherType))
      case p: Property => p.entity.toField
      case _ => None
    }
  }

  def discardScansForNonexistentLabels: PartialFunction[LogicalOperator, LogicalOperator] = {
    case scan@PatternScan(NodePattern(CTNode(labels, _)), mapping, in, _) =>
      def graphSchema = in.graph.schema

      def emptyRecords = {
        val fields = mapping.keySet.flatMap {
          case v: Var => Set(v)
          case _ => Set.empty[Var]
        }
        EmptyRecords(fields, in, scan.solved)
      }

      if ((labels.size == 1 && !graphSchema.labels.contains(labels.head)) ||
        (labels.size > 1 && !graphSchema.labelCombinations.combos.exists(labels.subsetOf(_)))) {
        emptyRecords
      } else {
        scan
      }
  }

  private def graphProvidesTripletPatternFor(
    exp: Expand,
    availablePatterns: Set[Pattern],
    g: QualifiedGraphName,
    context: LogicalPlannerContext
  ): Boolean = {
    val sourceLabels = exp.source.cypherType.toCTNode.labels
    val sourceCombos = context.resolveSchema(g).combinationsFor(sourceLabels)

    val relTypes = exp.rel.cypherType.toCTRelationship.types

    val targetLabels = exp.target.cypherType.toCTNode.labels
    val targetCombos = context.resolveSchema(g).combinationsFor(targetLabels)

    val combos = for {
      sourceCombo <- sourceCombos
      relType <- relTypes
      targetCombo <- targetCombos
    } yield (sourceCombo, relType, targetCombo)

    combos.forall {
      case(sourceCombo, relType, targetCombo) =>
        availablePatterns.exists {
          case TripletPattern(CTNode(source, _), CTRelationship(rel, _), CTNode(target, _)) =>
            source == sourceCombo && rel.contains(relType) && target == targetCombo
          case _ => false
        }
    } && combos.nonEmpty
  }

  private def graphProvidesNodeRelPatternFor(
    exp: Expand,
    availablePatterns: Set[Pattern],
    g: QualifiedGraphName,
    context: LogicalPlannerContext
  ): Boolean = {
    val sourceLabels = exp.source.cypherType.toCTNode.labels
    val sourceCombos = context.resolveSchema(g).combinationsFor(sourceLabels)

    val relTypes = exp.rel.cypherType.toCTRelationship.types

    val combos = for {
      sourceCombo <- sourceCombos
      relType <- relTypes
    } yield (sourceCombo, relType)

    combos.forall {
      case(sourceCombo, relType) =>
        availablePatterns.exists {
          case NodeRelPattern(CTNode(labels, _), CTRelationship(rel, _)) => labels == sourceCombo && rel.contains(relType)
          case _ => false
        }
    } && combos.nonEmpty
  }

}
