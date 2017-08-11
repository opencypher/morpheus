/**
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
package org.opencypher.spark.impl.logical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.pattern.{EveryNode, EveryRelationship}
import org.opencypher.spark.api.ir.{Field, SolvedQueryModel}
import org.opencypher.spark.api.record.{ProjectedExpr, ProjectedField}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.util._

class LogicalOperatorProducer {
  def planBoundedVarLengthExpand(source: Field, r: Field, types: EveryRelationship, target: Field, lower: Int, upper: Int, sourcePlan: LogicalOperator, targetPlan: LogicalOperator): BoundedVarLengthExpand = {
    val prevSolved = sourcePlan.solved ++ targetPlan.solved

    val solved = types.relTypes.elements.foldLeft(prevSolved.withField(r)) {
      case (acc, next) => acc.withPredicate(HasType(r, next)(CTBoolean))
    }

    BoundedVarLengthExpand(source, r, target, lower, upper, sourcePlan, targetPlan)(solved)
  }

  def planTargetExpand(source: Field, rel: Field, target: Field, sourcePlan: LogicalOperator, targetPlan: LogicalOperator): ExpandTarget = {
    val prevSolved = sourcePlan.solved ++ targetPlan.solved

    val solved = prevSolved.withField(rel)

    ExpandTarget(source, rel, target, sourcePlan, targetPlan)(solved)
  }

  def planSourceExpand(source: Field, rel: Field, types: EveryRelationship, target: Field, sourcePlan: LogicalOperator, targetPlan: LogicalOperator): ExpandSource = {

    val prevSolved = sourcePlan.solved ++ targetPlan.solved

    val solved = types.relTypes.elements.foldLeft(prevSolved.withField(rel)) {
      case (acc, next) => acc.withPredicate(HasType(rel, next)(CTBoolean))
    }

    ExpandSource(source, rel, types, target, sourcePlan, targetPlan)(solved)
  }

  def planExpandInto(source: Field, rel: Field, types: EveryRelationship, target: Field, sourcePlan: LogicalOperator): ExpandInto = {
    val solved = types.relTypes.elements.foldLeft(sourcePlan.solved.withField(rel)) {
      case (acc, next) => acc.withPredicate(HasType(rel, next)(CTBoolean))
    }

    ExpandInto(source, rel, types, target, sourcePlan)(solved)
  }

  def planNodeScan(node: Field, everyNode: EveryNode, prev: LogicalOperator): NodeScan = {
    val solved = everyNode.labels.elements.foldLeft(SolvedQueryModel.empty[Expr].withField(node)) {
      case (acc, label) => acc.withPredicate(HasLabel(node, label)(CTBoolean))
    }

    NodeScan(node, everyNode, prev)(solved)
  }

  def planFilter(expr: Expr, prev: LogicalOperator): Filter = {
    Filter(expr, prev)(prev.solved.withPredicate(expr))
  }

  def projectField(field: Field, expr: Expr, prev: LogicalOperator): Project = {
    val projection = ProjectedField(field, expr)

    Project(projection, prev)(prev.solved.withField(field))
  }

  def projectExpr(expr: Expr, prev: LogicalOperator): Project = {
    val projection = ProjectedExpr(expr)

    Project(projection, prev)(prev.solved)
  }

  def planSelect(fields: IndexedSeq[Var], prev: LogicalOperator): Select = {
    Select(fields, prev)(prev.solved)
  }

  def planStart(schema: Schema, fields: Set[Var]): Start = {
    val irFields = fields.map { v => Field(v.name)(v.cypherType) }
    Start(NamedLogicalGraph("default", schema), DefaultGraphSource, fields)(SolvedQueryModel(irFields, Set.empty))
  }
}
