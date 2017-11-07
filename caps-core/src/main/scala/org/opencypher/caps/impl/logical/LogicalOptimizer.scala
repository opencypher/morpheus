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
import org.opencypher.caps.api.types.{CTBoolean, CTNode, CTVoid}
import org.opencypher.caps.impl.DirectCompilationStage
import org.opencypher.caps.ir.api.Label

class LogicalOptimizer(producer: LogicalOperatorProducer)
  extends DirectCompilationStage[LogicalOperator, LogicalOperator, LogicalPlannerContext]{

  override def process(input: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    discardSelectVoid(moveLabelPredicatesToNodeScans(input))
  }

  /**
    * This rewriter discards a select when one of the selected variables has non-nullable type VOID.
    *
    * @param input logical plan
    * @return rewritten plan
    */
  private def discardSelectVoid(input: LogicalOperator): LogicalOperator = {
    input match {
        case s: Select if s.fields.map(_.cypherType).exists(t => t == CTVoid && t.isMaterial) =>
          EmptyRecords(s.fields, discardStackedRecordOperations(s.in))(s.solved)
        case b: BinaryLogicalOperator =>
          b.clone(discardSelectVoid(b.lhs), discardSelectVoid(b.rhs))
        case s: StackingLogicalOperator =>
          s.clone(discardSelectVoid(s.in))
        case l: LogicalLeafOperator =>
          l
      }

  }

  private def discardStackedRecordOperations(input: LogicalOperator): LogicalOperator = {
    input match {
      case s: SetSourceGraph => s.clone(discardStackedRecordOperations(s.in))
      case p: ProjectGraph => p.clone(discardStackedRecordOperations(p.in))
      case b: BinaryLogicalOperator =>
        b.clone(discardSelectVoid(b.lhs), discardSelectVoid(b.rhs))
      case s: StackingLogicalOperator =>
        discardStackedRecordOperations(s.in)
      case l: LogicalLeafOperator =>
        l
    }

  }


  /**
    * This rewriter removes node label filters from the plan and pushes the predicate down to the NodeScan operations.
    *
    * @param input logical plan
    * @return rewritten plan
    */
  private def moveLabelPredicatesToNodeScans(input: LogicalOperator): LogicalOperator = {

    def extractLabels(op: LogicalOperator): Set[(Var, Label)] = {
      op match {
        case Filter(expr, in) =>
          val res = expr match {
            case HasLabel(v: Var, label) => Set(v -> label)
            case _ => Set.empty
          }
          res ++ extractLabels(in)
        case s: StackingLogicalOperator =>
          extractLabels(s.in)
        case b: BinaryLogicalOperator =>
          extractLabels(b.lhs) ++ extractLabels(b.rhs)
        case _ => Set.empty
      }
    }

    val labelMap = extractLabels(input).groupBy(_._1).mapValues(_.map(_._2))

    def rewrite(root: LogicalOperator): LogicalOperator = {
      root match {
        case n@NodeScan(node, in) =>
          val labels = labelMap.getOrElse(node, Set.empty)
          val labelNames = labels.map(_.name)

          val nodeVar = Var(node.name)(CTNode(labelNames))
          val solved = in.solved.withPredicates(labels.map(l => HasLabel(nodeVar, l)(CTBoolean)).toSeq: _*)

          labelNames.size match {
            case 0 => n // No filter pushed in, return unchanged NodeScan
            case 1 =>
              if (in.sourceGraph.schema.labels.contains(labelNames.head)) {
                NodeScan(nodeVar, rewrite(in))(solved)
              } else {
                EmptyRecords(Set(nodeVar), discardStackedRecordOperations(in))(solved)
              }
            case _ =>
              val combinationsInGraph = in.sourceGraph.schema.labelCombinations.combos
              if (combinationsInGraph.exists(labelNames.subsetOf(_))) {
                NodeScan(nodeVar, rewrite(in))(solved)
              } else {
                EmptyRecords(Set(nodeVar), discardStackedRecordOperations(in))(solved)
              }
          }

        case f@Filter(expr, in) =>
          expr match {
            case _: HasLabel => rewrite(in)
            case _ => f.clone(rewrite(in))
          }
        case s: StackingLogicalOperator =>
          s.clone(rewrite(s.in))
        case b: BinaryLogicalOperator =>
          b.clone(rewrite(b.lhs), rewrite(b.rhs))
        case l: LogicalLeafOperator =>
          l
      }
    }
    rewrite(input)
  }
}
