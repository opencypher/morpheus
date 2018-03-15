/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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

import org.opencypher.okapi.ir.api.{IRCatalogGraph, IRField, IRGraph, IRPatternGraph}
import org.opencypher.okapi.ir.api.block.{Aggregations, SortItem}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.impl.util.VarConverters._

// TODO: Homogenize naming
// TODO: Align names with other producers
class LogicalOperatorProducer {

  def planCartesianProduct(lhs: LogicalOperator, rhs: LogicalOperator): CartesianProduct = {
    CartesianProduct(lhs, rhs, lhs.solved ++ rhs.solved)
  }

  def planValueJoin(
      lhs: LogicalOperator,
      rhs: LogicalOperator,
      predicates: Set[org.opencypher.okapi.ir.api.expr.Equals]): ValueJoin = {
    ValueJoin(lhs, rhs, predicates, predicates.foldLeft(lhs.solved ++ rhs.solved) {
      case (solved, predicate) => solved.withPredicate(predicate)
    })
  }

  def planBoundedVarLengthExpand(
      source: IRField,
      r: IRField,
      target: IRField,
      direction: Direction,
      lower: Int,
      upper: Int,
      sourcePlan: LogicalOperator,
      targetPlan: LogicalOperator): BoundedVarLengthExpand = {
    val prevSolved = sourcePlan.solved ++ targetPlan.solved

    BoundedVarLengthExpand(source, r, target, direction, lower, upper, sourcePlan, targetPlan, prevSolved.withField(r))
  }

  def planExpand(
      source: IRField,
      rel: IRField,
      target: IRField,
      direction: Direction,
      sourcePlan: LogicalOperator,
      targetPlan: LogicalOperator): Expand = {

    val prevSolved = sourcePlan.solved ++ targetPlan.solved

    Expand(source, rel, target, direction, sourcePlan, targetPlan, prevSolved.solveRelationship(rel))
  }

  def planExpandInto(source: IRField, rel: IRField, target: IRField, direction: Direction, sourcePlan: LogicalOperator): ExpandInto = {
    ExpandInto(source, rel, target, direction, sourcePlan, sourcePlan.solved.solveRelationship(rel))
  }

  def planNodeScan(node: IRField, prev: LogicalOperator): NodeScan = {
    NodeScan(node, prev, prev.solved.withField(node))
  }

  def planFilter(expr: Expr, prev: LogicalOperator): Filter = {
    Filter(expr, prev, prev.solved.withPredicate(expr))
  }

  def planDistinct(fields: Set[IRField], prev: LogicalOperator): Distinct = {
    Distinct(fields.map(toVar), prev, prev.solved)
  }

  def planOptional(nonOptionalPlan: LogicalOperator, optionalPlan: LogicalOperator): Optional = {
    Optional(nonOptionalPlan, optionalPlan, optionalPlan.solved)
  }

  def planExistsSubQuery(
      expr: ExistsPatternExpr,
      matchPlan: LogicalOperator,
      patternPlan: LogicalOperator): ExistsSubQuery = {
    ExistsSubQuery(expr, matchPlan, patternPlan, matchPlan.solved)
  }

  def aggregate(aggregations: Aggregations[Expr], group: Set[IRField], prev: LogicalOperator): Aggregate = {
    val transformed = aggregations.pairs.collect { case (field, aggregator: Aggregator) => toVar(field) -> aggregator }

    Aggregate(transformed, group.map(toVar), prev, prev.solved.withFields(aggregations.fields.toSeq: _*))
  }

  def projectField(field: IRField, expr: Expr, prev: LogicalOperator): Project = {
    Project(expr, Some(field), prev, prev.solved.withField(field))
  }

  def projectExpr(expr: Expr, prev: LogicalOperator): Project = {
    Project(expr, None, prev, prev.solved)
  }

  def planUnwind(list: Expr, variable: IRField, withList: LogicalOperator): Unwind = {
    Unwind(list, variable, withList, withList.solved.withField(variable))
  }

  def planSelect(fields: IndexedSeq[Var], prev: LogicalOperator): Select = {
    Select(fields, Set.empty, prev, prev.solved)
  }

  def planReturnGraph(prev: LogicalOperator): ReturnGraph = {
    ReturnGraph(prev, prev.solved)
  }

  def planUseGraph(graph: LogicalGraph, prev: LogicalOperator): UseGraph = {
    UseGraph(graph, prev, prev.solved)
  }

  def planStart(graph: LogicalGraph, fields: Set[Var]): Start = {
    val irFields = fields.map { v =>
      IRField(v.name)(v.cypherType)
    }
    Start(graph, fields, SolvedQueryModel(irFields))
  }

  def planOrderBy(sortItems: Seq[SortItem[Expr]], prev: LogicalOperator): OrderBy = {
    OrderBy(sortItems, prev, prev.solved)
  }

  def planSkip(expr: Expr, prev: LogicalOperator): Skip = {
    Skip(expr, prev, prev.solved)
  }

  def planLimit(expr: Expr, prev: LogicalOperator): Limit = {
    Limit(expr, prev, prev.solved)
  }

}
