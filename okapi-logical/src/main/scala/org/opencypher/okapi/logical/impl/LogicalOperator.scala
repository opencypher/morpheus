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
 */
package org.opencypher.okapi.logical.impl

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.ir.api.Label
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.trees.AbstractTreeNode

sealed abstract class LogicalOperator extends AbstractTreeNode[LogicalOperator] {
  def solved: SolvedQueryModel

  val fields: Set[Var]

  def graph: LogicalGraph

  override def args = super.args.filter {
    case SolvedQueryModel(_, _) => false
    case _ => true
  }
}

sealed trait LogicalGraph {
  def schema: Schema

  override def toString = s"${getClass.getSimpleName}($args)"

  protected def args: String
}

final case class LogicalCatalogGraph(qualifiedGraphName: QualifiedGraphName, schema: Schema) extends LogicalGraph {
  override protected def args: String = s"qualifiedGraphName = $qualifiedGraphName"
}

final case class LogicalPatternGraph(schema: Schema, entities: Set[ConstructedEntity]) extends LogicalGraph {
  override protected def args: String = entities.toString
}

sealed trait ConstructedEntity {
  def v: Var
}

case class ConstructedNode(v: Var, labels: Set[Label]) extends ConstructedEntity

case class ConstructedRelationship(v: Var, source: Var, target: Var, typ: String) extends ConstructedEntity

sealed abstract class StackingLogicalOperator extends LogicalOperator {
  def in: LogicalOperator

  override def graph: LogicalGraph = in.graph
}

sealed abstract class BinaryLogicalOperator extends LogicalOperator {
  def lhs: LogicalOperator

  def rhs: LogicalOperator

  /**
    * Always pick the source graph from the right-hand side, because it works for in-pattern expansions
    * and changing of source graphs. This relies on the planner always planning _later_ operators on the rhs.
    */
  override def graph: LogicalGraph = rhs.graph
}

sealed abstract class LogicalLeafOperator extends LogicalOperator

final case class NodeScan(node: Var, in: LogicalOperator, solved: SolvedQueryModel)
    extends StackingLogicalOperator {
  require(node.cypherType.isInstanceOf[CTNode], "A variable for a node scan needs to have type CTNode")

  def labels: Set[String] = node.cypherType.asInstanceOf[CTNode].labels

  override val fields: Set[Var] = in.fields + node
}

final case class Distinct(fields: Set[Var], in: LogicalOperator, solved: SolvedQueryModel)
    extends StackingLogicalOperator

final case class Filter(expr: Expr, in: LogicalOperator, solved: SolvedQueryModel)
    extends StackingLogicalOperator {

  // TODO: Add more precise type information based on predicates (?)
  override val fields: Set[Var] = in.fields
}

sealed trait ExpandOperator {
  def source: Var

  def rel: Var

  def target: Var

  def direction: Direction
}

final case class Expand(
    source: Var,
    rel: Var,
    target: Var,
    direction: Direction,
    lhs: LogicalOperator,
    rhs: LogicalOperator,
    solved: SolvedQueryModel)
    extends BinaryLogicalOperator
    with ExpandOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields + rel
}

final case class BoundedVarLengthExpand(
    source: Var,
    rel: Var,
    target: Var,
    direction: Direction,
    lower: Int,
    upper: Int,
    lhs: LogicalOperator,
    rhs: LogicalOperator,
    solved: SolvedQueryModel)
    extends BinaryLogicalOperator
    with ExpandOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields
}

final case class ValueJoin(
    lhs: LogicalOperator,
    rhs: LogicalOperator,
    predicates: Set[org.opencypher.okapi.ir.api.expr.Equals],
    solved: SolvedQueryModel)
    extends BinaryLogicalOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields
}

final case class ExpandInto(source: Var, rel: Var, target: Var, direction: Direction, in: LogicalOperator, solved: SolvedQueryModel)
    extends StackingLogicalOperator
    with ExpandOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields

  def lhs: LogicalOperator = in

  def rhs: LogicalOperator = in
}

final case class Project(expr: Expr, field: Option[Var], in: LogicalOperator, solved: SolvedQueryModel)
    extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields ++ field
}

final case class Unwind(expr: Expr, field: Var, in: LogicalOperator, solved: SolvedQueryModel)
    extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields + field
}

final case class Aggregate(
    aggregations: Set[(Var, Aggregator)],
    group: Set[Var],
    in: LogicalOperator,
    solved: SolvedQueryModel)
    extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields ++ aggregations.map(_._1) ++ group
}

final case class Select(
    orderedFields: IndexedSeq[Var],
    graphs: Set[String],
    in: LogicalOperator,
    solved: SolvedQueryModel)
    extends StackingLogicalOperator {

  override val fields: Set[Var] = orderedFields.toSet
}

final case class ReturnGraph(in: LogicalOperator, solved: SolvedQueryModel) extends StackingLogicalOperator {
  override val fields: Set[Var] = Set.empty
}

final case class OrderBy(sortItems: Seq[SortItem[Expr]], in: LogicalOperator, solved: SolvedQueryModel)
    extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields
}

final case class Skip(expr: Expr, in: LogicalOperator, solved: SolvedQueryModel) extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields
}

final case class Limit(expr: Expr, in: LogicalOperator, solved: SolvedQueryModel)
    extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields
}

final case class CartesianProduct(lhs: LogicalOperator, rhs: LogicalOperator, solved: SolvedQueryModel)
    extends BinaryLogicalOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields
}

final case class Optional(lhs: LogicalOperator, rhs: LogicalOperator, solved: SolvedQueryModel)
    extends BinaryLogicalOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields
}

final case class ExistsSubQuery(
    expr: ExistsPatternExpr,
    lhs: LogicalOperator,
    rhs: LogicalOperator,
    solved: SolvedQueryModel)
    extends BinaryLogicalOperator {

  override val fields: Set[Var] = lhs.fields + expr.targetField
}

final case class UseGraph(
    override val graph: LogicalGraph,
    in: LogicalOperator,
    solved: SolvedQueryModel)
    extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields
}

final case class EmptyRecords(fields: Set[Var], in: LogicalOperator, solved: SolvedQueryModel)
    extends StackingLogicalOperator

final case class Start(graph: LogicalGraph, fields: Set[Var], solved: SolvedQueryModel)
    extends LogicalLeafOperator
