/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.caps.impl.logical

import org.opencypher.caps.api.expr.{HasLabel, Var}
import org.opencypher.caps.api.types.{CTBoolean, CTNode, CTNull}
import org.opencypher.caps.impl.DirectCompilationStage
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.ir.api.Label

class LogicalOptimizer(producer: LogicalOperatorProducer)
  extends DirectCompilationStage[LogicalOperator, LogicalOperator, LogicalPlannerContext] {

  override def process(input: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    val labelMap = ExtractLabels(input).groupBy(_._1).mapValues(_.map(_._2.name))
    val pushedLabels = PushLabelFiltersIntoScans(labelMap)(input)
    val discardScan = DiscardNodeScanForInexistentLabel(pushedLabels)
    discardScan
  }

}

trait LogicalRewriter extends Function1[LogicalOperator, LogicalOperator] {
  def apply(root: LogicalOperator): LogicalOperator

  def rewriteChildren(child: LogicalOperator, r: LogicalRewriter = this): LogicalOperator = {
    child match {
      case b: BinaryLogicalOperator =>
        b.clone(r(b.lhs), r(b.rhs))
      case s: StackingLogicalOperator =>
        s.clone(r(s.in))
      case l: LogicalLeafOperator =>
        l
    }
  }
}

trait LogicalAggregator[A] extends Function1[LogicalOperator, A] {
  def apply(root: LogicalOperator): A
}

object ExtractLabels extends LogicalAggregator[Set[(Var, Label)]] {
  def apply(root: LogicalOperator): Set[(Var, Label)] = {
    root match {
      case Filter(expr, in) =>
        val res = expr match {
          case HasLabel(v: Var, label) => Set(v -> label)
          case _ => Set.empty
        }
        res ++ ExtractLabels(in)
      case s: StackingLogicalOperator =>
        ExtractLabels(s.in)
      case b: BinaryLogicalOperator =>
        ExtractLabels(b.lhs) ++ ExtractLabels(b.rhs)
      case _ => Set.empty
    }
  }
}

case object DiscardStackedRecordOperations extends LogicalRewriter {
  override def apply(root: LogicalOperator): LogicalOperator = {
    root match {
      case s: SetSourceGraph => s.clone(DiscardStackedRecordOperations(s.in))
      case p: ProjectGraph => p.clone(DiscardStackedRecordOperations(p.in))
      case s: StackingLogicalOperator => DiscardStackedRecordOperations(s.in)
      case other => rewriteChildren(other)
    }
  }
}

case class PushLabelFiltersIntoScans(labelMap: Map[Var, Set[String]]) extends LogicalRewriter {
  override def apply(root: LogicalOperator): LogicalOperator = {
    root match {
      case n@NodeScan(node, in) =>
        val labels = labelMap.getOrElse(node, Set.empty)
        val nodeVar = Var(node.name)(CTNode(labels))
        val solved = in.solved.withPredicates(labels.map(l => HasLabel(nodeVar, Label(l))(CTBoolean)).toSeq: _*)
        NodeScan(nodeVar, this (in))(solved)
      case f@Filter(expr, in) =>
        expr match {
          case _: HasLabel => this (in)
          case _ => f.clone(this (in))
        }
      case other => rewriteChildren(other)
    }
  }
}

case object DiscardNodeScanForInexistentLabel extends LogicalRewriter {
  override def apply(root: LogicalOperator): LogicalOperator = {
    root match {
      case s@NodeScan(v, in) =>
        v.cypherType match {
          case CTNode(labels) =>
            labels.size match {
              case 0 => s
              case 1 =>
                if (in.sourceGraph.schema.labels.contains(labels.head)) {
                  s
                } else {
                  EmptyRecords(Set(v), DiscardStackedRecordOperations(in))(s.solved)
                }
              case _ =>
                val combinationsInGraph = in.sourceGraph.schema.labelCombinations.combos
                if (combinationsInGraph.exists(labels.subsetOf(_))) {
                  NodeScan(v, in)(s.solved)
                } else {
                  EmptyRecords(Set(v), DiscardStackedRecordOperations(in))(s.solved)
                }
            }
          case _ => Raise.impossible(s"NodeScan on non-node ${v.cypherType}")
        }
      case other => rewriteChildren(other)
    }
  }
}
