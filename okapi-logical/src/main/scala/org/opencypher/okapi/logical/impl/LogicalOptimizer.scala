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

import org.opencypher.okapi.api.io.{NodeRelPattern, PatternProvider}
import org.opencypher.okapi.api.types.{CTBoolean, CTNode}
import org.opencypher.okapi.ir.api.IRField
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.trees.{BottomUp, BottomUpWithContext}

object LogicalOptimizer extends DirectCompilationStage[LogicalOperator, LogicalOperator, LogicalPlannerContext] {

  override def process(input: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    val optimizationRules = Seq(
      discardScansForNonexistentLabels,
      replaceCartesianWithValueJoin,
      replaceScansWithRecognizePatterns
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
          val joinExpr = Equals(leftExpr, rightExpr)(CTBoolean)
          val leftProject = Project(leftExpr -> None, lhs, lhs.solved)
          val rightProject = Project(rightExpr -> None, rhs, rhs.solved)
          ValueJoin(leftProject, rightProject, Set(joinExpr), solved.withPredicate(joinExpr)) -> true
      }.transform(in, context = false)

      if (rewritten) newChild else filter
  }

  def replaceScansWithRecognizePatterns(implicit context: LogicalPlannerContext): PartialFunction[LogicalOperator, LogicalOperator] = {
    case exp: Expand =>
      exp.source.cypherType.graph.flatMap { g =>
        val availablePatterns = context.catalog(g.namespace) match {
          case p: PatternProvider =>
            p.patterns(g.graphName)
          case _ => Seq.empty
        }

        val firstMatchingPattern = availablePatterns.find {
          case NodeRelPattern(node, rel) if node.withGraph(g) == exp.source.cypherType && rel.withGraph(g) == exp.rel.cypherType => true
          case _ => false
        }

        firstMatchingPattern.map { pattern =>
          val withPatternScan = BottomUp[LogicalOperator](replaceNodeScanWithPatternScan(exp.source, exp.rel, pattern)).transform(exp.lhs)
          val joinExpr = Equals(EndNode(exp.rel)(CTNode), exp.target)(CTBoolean)
          ValueJoin(withPatternScan, exp.rhs, Set(joinExpr), exp.solved)
        }
      }.getOrElse(exp)
  }

  private def replaceNodeScanWithPatternScan(node: Var, rel: Var, pattern: NodeRelPattern): PartialFunction[LogicalOperator, LogicalOperator] = {
    case NodeScan(n, parent, solved) if n == node =>
      PatternScan(node, rel, pattern, parent, solved.withFields(node.toField.get, rel.toField.get))

    case pScan: PatternScan if pScan.node == node && pScan.rel != rel =>
      val renamedNode = Var(node.name + "_renamed")(node.cypherType)
      val otherPaternScan =PatternScan(renamedNode, rel, pattern, pScan.in, pScan.in.solved.withFields(node.toField.get, rel.toField.get))

      val joinExpr = Equals(pScan.node, otherPaternScan.node)(CTBoolean)
      val joinOp = ValueJoin(pScan, otherPaternScan, Set(joinExpr), pScan.solved ++ otherPaternScan.solved)
      Select(List(pScan.node, pScan.rel, otherPaternScan.rel), joinOp, joinOp.solved)
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
      case Property(e: Var, _) => e.toField
      case _ => None
    }
  }

  def discardScansForNonexistentLabels: PartialFunction[LogicalOperator, LogicalOperator] = {
    case scan@NodeScan(entityExpr, in, _) =>
      def graphSchema = in.graph.schema

      def emptyRecords = {
        val fields = entityExpr match {
          case v: Var => Set(v)
          case _ => Set.empty[Var]
        }
        EmptyRecords(fields, in, scan.solved)
      }

      if ((scan.labels.size == 1 && !graphSchema.labels.contains(scan.labels.head)) ||
        (scan.labels.size > 1 && !graphSchema.labelCombinations.combos.exists(scan.labels.subsetOf(_)))) {
        emptyRecords
      } else {
        scan
      }
  }

}
