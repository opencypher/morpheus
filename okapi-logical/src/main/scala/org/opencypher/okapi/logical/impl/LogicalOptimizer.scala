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
package org.opencypher.okapi.logical.impl

import org.opencypher.okapi.api.types.{CTBoolean, CTNode}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.ir.api.{IRField, Label}
import org.opencypher.okapi.trees.{BottomUp, BottomUpWithContext}

object LogicalOptimizer extends DirectCompilationStage[LogicalOperator, LogicalOperator, LogicalPlannerContext] {

  override def process(input: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    val optimizationRules = Seq(pushLabelsIntoScans(labelsForVariables(input)), discardScansForNonexistentLabels, replaceCartesianWithValueJoin)
    optimizationRules.foldLeft(input) {
      // TODO: Evaluate if multiple rewriters could be fused
      case (tree: LogicalOperator, optimizationRule) => BottomUp[LogicalOperator](optimizationRule).transform(tree)
    }
  }

  def labelsForVariables(r: LogicalOperator): Map[Var, Set[String]] = {
    r.foldLeft(Map.empty[Var, Set[String]].withDefaultValue(Set.empty)) {
      case (r, n) =>
        n match {
          case Filter(HasLabel(v: Var, Label(name)), _, _) => r.updated(v, r(v) + name)
          case _ => r
        }
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

  def pushLabelsIntoScans(labelMap: Map[Var, Set[String]]): PartialFunction[LogicalOperator, LogicalOperator] = {
    case ns@NodeScan(v@Var(name), in, _) =>
      val updatedLabels = labelMap(v)
      val updatedVar = Var(name)(CTNode(ns.labels ++ updatedLabels, v.cypherType.graph))
      val updatedSolved = in.solved.withPredicates(updatedLabels.map(l => HasLabel(v, Label(l))(CTBoolean)).toSeq: _*)
      NodeScan(updatedVar, in, updatedSolved)
    case Filter(_: HasLabel, in, _) => in
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
