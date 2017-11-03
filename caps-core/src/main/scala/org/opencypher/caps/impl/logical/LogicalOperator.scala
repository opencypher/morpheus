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

import java.net.URI

import org.opencypher.caps.api.expr._
import org.opencypher.caps.ir.api.{Label, SolvedQueryModel}
import org.opencypher.caps.ir.api.block.SortItem
import org.opencypher.caps.ir.api.pattern.{EveryNode, EveryRelationship, Pattern}
import org.opencypher.caps.api.record.ProjectedSlotContent
import org.opencypher.caps.api.schema.Schema

import scala.language.implicitConversions

sealed trait LogicalOperator {
  def isLeaf = false
  def solved: SolvedQueryModel[Expr]

  val fields: Set[Var]
  def sourceGraph: LogicalGraph

  protected def prefix(depth: Int): String = ("· " * depth ) + "|-"
  def pretty(depth: Int = 0): String
}

trait LogicalGraph {
  def schema: Schema
  def name: String
}

final case class LogicalExternalGraph(name: String, uri: URI, schema: Schema) extends LogicalGraph {
  override def toString: String = s"GRAPH $name AT $uri"
}

final case class LogicalPatternGraph(name: String, schema: Schema, pattern: GraphOfPattern) extends LogicalGraph {
  override def toString: String = s"GRAPH $name OF $pattern"
}

final case class GraphOfPattern(toCreate: Set[ConstructedEntity], toRetain: Set[Var])

sealed trait ConstructedEntity {
  def v: Var
}

case class ConstructedNode(v: Var, labels: Set[Label]) extends ConstructedEntity
case class ConstructedRelationship(v: Var, source: Var, target: Var, typ: String) extends ConstructedEntity

sealed trait StackingLogicalOperator extends LogicalOperator {
  def in: LogicalOperator

  override def sourceGraph: LogicalGraph = in.sourceGraph

  def clone(newIn: LogicalOperator = in): LogicalOperator
}

sealed trait BinaryLogicalOperator extends LogicalOperator {
  def lhs: LogicalOperator
  def rhs: LogicalOperator

  /**
   * Always pick the source graph from the right-hand side, because it works for in-pattern expansions
   * and changing of source graphs. This relies on the planner always planning _later_ operators on the rhs.
   */
  override def sourceGraph: LogicalGraph = rhs.sourceGraph

  def clone(newLhs: LogicalOperator = lhs, newRhs: LogicalOperator = rhs): LogicalOperator
}

sealed trait LogicalLeafOperator extends LogicalOperator {
  override def isLeaf = true
}

final case class NodeScan(node: Var, nodeDef: EveryNode, in: LogicalOperator)
                         (override val solved: SolvedQueryModel[Expr])
  extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields + node

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} NodeScan(node = $node, nodeDef: $nodeDef)
       #${in.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newIn: LogicalOperator = in): LogicalOperator = copy(in = newIn)(solved)

}

final case class Distinct(fields: Set[Var], in: LogicalOperator)
                         (override val solved: SolvedQueryModel[Expr])
  extends StackingLogicalOperator {

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} Distinct(fields = $fields)
       #${in.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newIn: LogicalOperator = in): LogicalOperator = copy(in = newIn)(solved)
}

final case class Filter(expr: Expr, in: LogicalOperator)
                       (override val solved: SolvedQueryModel[Expr])
  extends StackingLogicalOperator {

  // TODO: Add more precise type information based on predicates (?)
  override val fields: Set[Var] = in.fields

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} Filter(expr = $expr)
       #${in.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newIn: LogicalOperator = in): LogicalOperator = copy(in = newIn)(solved)
}

sealed trait ExpandOperator extends BinaryLogicalOperator {
  def source: Var
  def rel: Var
  def target: Var

  def sourceOp: LogicalOperator
  def targetOp: LogicalOperator
}

final case class ExpandSource(source: Var, rel: Var, types: EveryRelationship, target: Var,
                              sourceOp: LogicalOperator, targetOp: LogicalOperator)
                             (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields + rel

  override def lhs: LogicalOperator = sourceOp
  override def rhs: LogicalOperator = targetOp

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} ExpandSource(source = $source, rel = $rel, target = $target)
       #${sourceOp.pretty(depth + 1)}
       #${targetOp.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newLhs: LogicalOperator = lhs, newRhs: LogicalOperator = rhs): LogicalOperator =
    copy(sourceOp = newLhs, targetOp = newRhs)(solved)
}

final case class ExpandTarget(source: Var, rel: Var, target: Var,
                              sourceOp: LogicalOperator, targetOp: LogicalOperator)
                             (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields

  override def lhs: LogicalOperator = targetOp
  override def rhs: LogicalOperator = sourceOp

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} ExpandTarget(source = $source, rel = $rel, target = $target)
       #${sourceOp.pretty(depth + 1)}
       #${targetOp.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newLhs: LogicalOperator = lhs, newRhs: LogicalOperator= rhs): LogicalOperator =
    copy(sourceOp = newLhs, targetOp = newRhs)(solved)
}

final case class BoundedVarLengthExpand(source: Var, rel: Var, target: Var,
                                        lower: Int, upper: Int,
                                        sourceOp: LogicalOperator, targetOp: LogicalOperator)
                                       (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields

  override def lhs: LogicalOperator = sourceOp
  override def rhs: LogicalOperator = targetOp

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} VarExpand(source = $source, rel = $rel, target = $target, lower = $lower, upper = $upper)
       #${sourceOp.pretty(depth + 1)}
       #${targetOp.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newLhs: LogicalOperator = lhs, newRhs: LogicalOperator = rhs): LogicalOperator =
    copy(sourceOp = newLhs, targetOp = newRhs)(solved)
}

final case class ValueJoin(lhs: LogicalOperator, rhs: LogicalOperator, predicates: Set[org.opencypher.caps.api.expr.Equals])
                          (override val solved: SolvedQueryModel[Expr])
  extends BinaryLogicalOperator {

  override def clone(newLhs: LogicalOperator, newRhs: LogicalOperator): ValueJoin =
    copy(newLhs, newRhs)(solved)

  override val fields: Set[Var] = lhs.fields ++ rhs.fields

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} ValueJoin(predicates = ${predicates.mkString("[", ", ", "]")})
       #${lhs.pretty(depth + 1)}
       #${rhs.pretty(depth + 1)}""".stripMargin('#')
}

final case class ExpandInto(source: Var, rel: Var, types: EveryRelationship, target: Var, sourceOp: LogicalOperator)
                           (override val solved: SolvedQueryModel[Expr])
  extends ExpandOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields

  override def targetOp: LogicalOperator = sourceOp

  override def lhs: LogicalOperator = sourceOp
  override def rhs: LogicalOperator = targetOp

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} ExpandInto(source = $source, rel = $rel)
       #${sourceOp.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newLhs: LogicalOperator = lhs, newRhs: LogicalOperator = rhs): LogicalOperator =
    copy(sourceOp = newLhs)(solved)
}

final case class Project(it: ProjectedSlotContent, in: LogicalOperator)
                        (override val solved: SolvedQueryModel[Expr])
  extends StackingLogicalOperator {

  override val fields: Set[Var] = it.alias.map(in.fields + _).getOrElse(in.fields)

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} Project(slotContent = $it)
       #${in.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newIn: LogicalOperator = in): LogicalOperator = copy(in = newIn)(solved)
}

final case class ProjectGraph(graph: LogicalGraph, in: LogicalOperator)
                             (override val solved: SolvedQueryModel[Expr])
  extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} ProjectGraph(graph = $graph)
       #${in.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newIn: LogicalOperator = in): LogicalOperator = copy(in = newIn)(solved)
}

final case class Aggregate(aggregations: Set[(Var, Aggregator)], group: Set[Var], in: LogicalOperator)
                          (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields ++ aggregations.map(_._1) ++ group

  override def clone(newIn: LogicalOperator): LogicalOperator = copy(in = newIn)(solved)

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} Aggregate(aggregations = ${aggregations.map(p => s"${p._2} AS ${p._1}").mkString(", ")})
       #${in.pretty(depth + 1)}""".stripMargin('#')

}

final case class Select(orderedFields: IndexedSeq[Var], graphs: Set[String], in: LogicalOperator)
                       (override val solved: SolvedQueryModel[Expr])
  extends StackingLogicalOperator {

  override val fields: Set[Var] = orderedFields.toSet

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} Select(fields = ${orderedFields.mkString(", ")})
       #${in.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newIn: LogicalOperator = in): LogicalOperator = copy(in = newIn)(solved)
}

final case class OrderBy(sortItems: Seq[SortItem[Expr]], in: LogicalOperator)
                        (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} OrderByAndSlice(sortItems = ${sortItems.mkString(", ")})
       #${in.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newIn: LogicalOperator): LogicalOperator = copy(in = newIn)(solved)
}

final case class Skip(expr: Expr, in: LogicalOperator)
                     (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} Skip(expr = $expr})
       #${in.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newIn: LogicalOperator): LogicalOperator = copy(in = newIn)(solved)
}

final case class Limit(expr: Expr, in: LogicalOperator)
                      (override val solved: SolvedQueryModel[Expr]) extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} Limit(expr = $expr})
       #${in.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newIn: LogicalOperator): LogicalOperator = copy(in = newIn)(solved)
}

final case class CartesianProduct(lhs: LogicalOperator, rhs: LogicalOperator)(override val solved: SolvedQueryModel[Expr])
  extends BinaryLogicalOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} CartesianProduct()
       #${lhs.pretty(depth + 1)}
       #${rhs.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newLhs: LogicalOperator, newRhs: LogicalOperator): LogicalOperator =
    copy(lhs = newLhs, rhs = newRhs)(solved)
}

final case class Optional(lhs: LogicalOperator, rhs: LogicalOperator)
                         (override val solved: SolvedQueryModel[Expr])
  extends BinaryLogicalOperator {

  override val fields: Set[Var] = lhs.fields ++ rhs.fields

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} Optional()
       #${rhs.pretty(depth + 1)}""".stripMargin('#')

  override def clone(newLhs: LogicalOperator, newRhs: LogicalOperator): LogicalOperator =
    copy(lhs = newLhs, rhs = newRhs)(solved)
}

final case class SetSourceGraph(override val sourceGraph: LogicalGraph, in: LogicalOperator)
                               (override val solved: SolvedQueryModel[Expr])
  extends StackingLogicalOperator {

  override val fields: Set[Var] = in.fields

  override def clone(newIn: LogicalOperator): SetSourceGraph = copy(in = newIn)(solved)

  override def pretty(depth: Int): String =
    s"""${prefix(depth)} SetSourceGraph(to = $sourceGraph)
       #${in.pretty(depth + 1)}""".stripMargin('#')

}

final case class Start(sourceGraph: LogicalGraph, fields: Set[Var])
                      (override val solved: SolvedQueryModel[Expr]) extends LogicalLeafOperator {

  override def pretty(depth: Int): String = s"${prefix(depth)} Start(at = $sourceGraph, with = $fields)"

  override def clone(): Start = copy()(solved)
}
