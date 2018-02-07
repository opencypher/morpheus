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
package org.opencypher.caps.impl.spark.physical

import java.net.URI

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.caps.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.CTRelationship
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.impl.flat
import org.opencypher.caps.impl.flat.FlatOperator
import org.opencypher.caps.impl.record._
import org.opencypher.caps.impl.spark.CAPSRecords
import org.opencypher.caps.impl.spark.physical.operators._
import org.opencypher.caps.ir.api.block.SortItem
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.api.util.DirectCompilationStage
import org.opencypher.caps.logical.impl._

trait PhysicalPlannerContext[R <: CypherRecords] {
  def session: CypherSession

  def resolver: URI => PropertyGraph

  def inputRecords: R

  def parameters: CypherMap
}

case class SparkPhysicalPlannerContext(
  session: CAPSSession,
  resolver: URI => PropertyGraph,
  inputRecords: CAPSRecords,
  parameters: CypherMap) extends PhysicalPlannerContext[CAPSRecords]

object SparkPhysicalPlannerContext {
  def from(
    resolver: URI => PropertyGraph,
    inputRecords: CAPSRecords,
    parameters: CypherMap)(implicit session: CAPSSession): PhysicalPlannerContext[CAPSRecords] = {
    SparkPhysicalPlannerContext(session, resolver, inputRecords, parameters)
  }
}

trait PhysicalOperatorProducer[P <: PhysicalOperator, R <: CypherRecords] {
  def planCartesianProduct(lhs: P, rhs: P, header: RecordHeader): P

  def planRemoveAliases(in: P, dependent: Set[(ProjectedField, ProjectedExpr)], header: RecordHeader): P

  def planSelectFields(in: P, fields: IndexedSeq[Var], header: RecordHeader): P

  def planSelectGraphs(in: P, graphs: Set[String]): P

  def planEmptyRecords(in: P, header: RecordHeader): P

  def planStart(in: R, g: LogicalExternalGraph): P

  def planSetSourceGraph(in: P, g: LogicalExternalGraph): P

  def planNodeScan(in: P, inGraph: LogicalGraph, v: Var, header: RecordHeader): P

  def planRelationshipScan(in: P, inGraph: LogicalGraph, v: Var, header: RecordHeader): P

  def planAlias(in: P, expr: Expr, alias: Var, header: RecordHeader): P

  def planUnwind(in: P, list: Expr, item: Var, header: RecordHeader): P

  def planProject(in: P, expr: Expr, header: RecordHeader): P

  def planProjectExternalGraph(in: P, name: String, uri: URI): P

  def planProjectPatternGraph(
    in: P,
    toCreate: Set[ConstructedEntity],
    name: String,
    schema: Schema,
    header: RecordHeader): P

  def planAggregate(in: P, aggregations: Set[(Var, Aggregator)], group: Set[Var], header: RecordHeader): P

  def planFilter(in: P, expr: Expr, header: RecordHeader): P

  def planValueJoin(lhs: P, rhs: P, predicates: Set[org.opencypher.caps.ir.api.expr.Equals], header: RecordHeader): P

  def planDistinct(in: P, fields: Set[Var]): P

  def planStartFromUnit(graph: LogicalExternalGraph): P

  def planExpandSource(
    first: P,
    second: P,
    third: P,
    source: Var,
    rel: Var,
    target: Var,
    header: RecordHeader,
    removeSelfRelationships: Boolean = false): P

  def planUnion(lhs: P, rhs: P): P

  def planExpandInto(lhs: P, rhs: P, source: Var, rel: Var, target: Var, header: RecordHeader): P

  def planInitVarExpand(in: P, source: Var, edgeList: Var, target: Var, header: RecordHeader): P

  def planBoundedVarExpand(
    first: P,
    second: P,
    third: P,
    rel: Var,
    edgeList: Var,
    target: Var,
    initialEndNode: Var,
    lower: Int,
    upper: Int,
    direction: Direction,
    header: RecordHeader,
    isExpandInto: Boolean): P

  def planOptional(lhs: P, rhs: P, header: RecordHeader): P

  def planExistsSubQuery(lhs: P, rhs: P, targetField: Var, header: RecordHeader): P

  def planOrderBy(in: P, sortItems: Seq[SortItem[Expr]], header: RecordHeader): P

  def planSkip(in: P, expr: Expr, header: RecordHeader): P

  def planLimit(in: P, expr: Expr, header: RecordHeader): P
}

final class SparkPhysicalOperatorProducer(implicit caps: CAPSSession)
  extends PhysicalOperatorProducer[PhysicalOperator, CAPSRecords] {

  override def planCartesianProduct(
    lhs: PhysicalOperator,
    rhs: PhysicalOperator,
    header: RecordHeader): PhysicalOperator = operators.CartesianProduct(lhs, rhs, header)

  override def planRemoveAliases(
    in: PhysicalOperator,
    dependent: Set[(ProjectedField, ProjectedExpr)],
    header: RecordHeader): PhysicalOperator = operators.RemoveAliases(in, dependent, header)

  override def planSelectFields(in: PhysicalOperator, fields: IndexedSeq[Var], header: RecordHeader): PhysicalOperator =
    operators.SelectFields(in, fields, header)

  override def planSelectGraphs(in: PhysicalOperator, graphs: Set[String]): PhysicalOperator =
    operators.SelectGraphs(in, graphs)

  override def planEmptyRecords(in: PhysicalOperator, header: RecordHeader): PhysicalOperator =
    operators.EmptyRecords(in, header)

  override def planStart(in: CAPSRecords, g: LogicalExternalGraph): PhysicalOperator =
    operators.Start(in, g)

  override def planSetSourceGraph(in: PhysicalOperator, g: LogicalExternalGraph): PhysicalOperator =
    operators.SetSourceGraph(in, g)

  override def planNodeScan(
    in: PhysicalOperator,
    inGraph: LogicalGraph,
    v: Var,
    header: RecordHeader): PhysicalOperator = operators.Scan(in, inGraph, v, header)

  override def planRelationshipScan(
    in: PhysicalOperator,
    inGraph: LogicalGraph,
    v: Var,
    header: RecordHeader): PhysicalOperator = operators.Scan(in, inGraph, v, header)

  override def planAlias(in: PhysicalOperator, expr: Expr, alias: Var, header: RecordHeader): PhysicalOperator =
    operators.Alias(in, expr, alias, header)

  override def planUnwind(in: PhysicalOperator, list: Expr, item: Var, header: RecordHeader): PhysicalOperator =
    operators.Unwind(in, list, item, header)

  override def planProject(in: PhysicalOperator, expr: Expr, header: RecordHeader): PhysicalOperator =
    operators.Project(in, expr, header)

  override def planProjectExternalGraph(in: PhysicalOperator, name: String, uri: URI): PhysicalOperator =
    operators.ProjectExternalGraph(in, name, uri)

  override def planProjectPatternGraph(
    in: PhysicalOperator,
    toCreate: Set[ConstructedEntity],
    name: String,
    schema: Schema,
    header: RecordHeader): PhysicalOperator = operators.ProjectPatternGraph(in, toCreate, name, schema, header)

  override def planAggregate(
    in: PhysicalOperator,
    aggregations: Set[(Var, Aggregator)],
    group: Set[Var],
    header: RecordHeader): PhysicalOperator = operators.Aggregate(in, aggregations, group, header)

  override def planFilter(in: PhysicalOperator, expr: Expr, header: RecordHeader): PhysicalOperator =
    operators.Filter(in, expr, header)

  override def planValueJoin(
    lhs: PhysicalOperator,
    rhs: PhysicalOperator,
    predicates: Set[Equals],
    header: RecordHeader): PhysicalOperator = operators.ValueJoin(lhs, rhs, predicates, header)

  override def planDistinct(in: PhysicalOperator, fields: Set[Var]): PhysicalOperator =
    operators.Distinct(in, fields)

  override def planStartFromUnit(graph: LogicalExternalGraph): PhysicalOperator =
    operators.StartFromUnit(graph)

  override def planExpandSource(
    first: PhysicalOperator,
    second: PhysicalOperator,
    third: PhysicalOperator,
    source: Var,
    rel: Var,
    target: Var,
    header: RecordHeader,
    removeSelfRelationships: Boolean): PhysicalOperator = operators.ExpandSource(
    first, second, third, source, rel, target, header, removeSelfRelationships)

  override def planUnion(lhs: PhysicalOperator, rhs: PhysicalOperator): PhysicalOperator =
    operators.Union(lhs, rhs)

  override def planExpandInto(
    lhs: PhysicalOperator,
    rhs: PhysicalOperator,
    source: Var,
    rel: Var,
    target: Var,
    header: RecordHeader): PhysicalOperator = operators.ExpandInto(lhs, rhs, source, rel, target, header)

  override def planInitVarExpand(
    in: PhysicalOperator,
    source: Var,
    edgeList: Var,
    target: Var,
    header: RecordHeader): PhysicalOperator = operators.InitVarExpand(in, source, edgeList, target, header)

  override def planBoundedVarExpand(
    first: PhysicalOperator,
    second: PhysicalOperator,
    third: PhysicalOperator,
    rel: Var,
    edgeList: Var,
    target: Var,
    initialEndNode: Var,
    lower: Int,
    upper: Int,
    direction: Direction,
    header: RecordHeader,
    isExpandInto: Boolean): PhysicalOperator = operators.BoundedVarExpand(
    first, second, third, rel, edgeList, target, initialEndNode, lower, upper, direction, header, isExpandInto)

  override def planOptional(lhs: PhysicalOperator, rhs: PhysicalOperator, header: RecordHeader): PhysicalOperator =
    operators.Optional(lhs, rhs, header)

  override def planExistsSubQuery(
    lhs: PhysicalOperator,
    rhs: PhysicalOperator,
    targetField: Var,
    header: RecordHeader): PhysicalOperator = operators.ExistsSubQuery(lhs, rhs, targetField, header)

  override def planOrderBy(
    in: PhysicalOperator,
    sortItems: Seq[SortItem[Expr]],
    header: RecordHeader): PhysicalOperator = operators.OrderBy(in, sortItems, header)

  override def planSkip(in: PhysicalOperator, expr: Expr, header: RecordHeader): PhysicalOperator =
    operators.Skip(in, expr, header)

  override def planLimit(in: PhysicalOperator, expr: Expr, header: RecordHeader): PhysicalOperator =
    operators.Limit(in, expr, header)
}

class PhysicalPlanner[P <: PhysicalOperator, R <: CypherRecords](producer: PhysicalOperatorProducer[P, R])
  extends DirectCompilationStage[FlatOperator, P, PhysicalPlannerContext[R]] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext[R]): P = {

    implicit val caps: CypherSession = context.session

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs, header) =>
        producer.planCartesianProduct(process(lhs), process(rhs), header)

      case flat.RemoveAliases(dependent, in, header) =>
        producer.planRemoveAliases(process(in), dependent, header)

      case flat.Select(fields, graphs: Set[String], in, header) =>
        val selected = producer.planSelectFields(process(in), fields, header)
        producer.planSelectGraphs(selected, graphs)

      case flat.EmptyRecords(in, header) =>
        producer.planEmptyRecords(process(in), header)

      case flat.Start(graph, _) =>
        graph match {
          case g: LogicalExternalGraph =>
            producer.planStart(context.inputRecords, g)

          case _ => throw IllegalArgumentException("a LogicalExternalGraph", graph)
        }

      case flat.SetSourceGraph(graph, in, _) =>
        graph match {
          case g: LogicalExternalGraph =>
            producer.planSetSourceGraph(process(in), g)

          case _ => throw IllegalArgumentException("a LogicalExternalGraph", graph)
        }

      case op@flat.NodeScan(v, in, header) =>
        producer.planNodeScan(process(in), op.sourceGraph, v, header)

      case op@flat.EdgeScan(e, in, header) =>
        producer.planRelationshipScan(process(in), op.sourceGraph, e, header)

      case flat.Alias(expr, alias, in, header) =>
        producer.planAlias(process(in), expr, alias, header)

      case flat.Unwind(list, item, in, header) =>
        producer.planUnwind(process(in), list, item, header)

      case flat.Project(expr, in, header) =>
        producer.planProject(process(in), expr, header)

      case flat.ProjectGraph(graph, in, header) =>
        graph match {
          case LogicalExternalGraph(name, uri, _) =>
            producer.planProjectExternalGraph(process(in), name, uri)
          case LogicalPatternGraph(name, targetSchema, GraphOfPattern(toCreate, _)) =>
            producer.planProjectPatternGraph(process(in), toCreate, name, targetSchema, header)
        }

      case flat.Aggregate(aggregations, group, in, header) =>
        producer.planAggregate(process(in), aggregations, group, header)

      case flat.Filter(expr, in, header) =>
        expr match {
          case TrueLit() =>
            process(in) // optimise away filter
          case _ =>
            producer.planFilter(process(in), expr, header)
        }

      case flat.ValueJoin(lhs, rhs, predicates, header) =>
        producer.planValueJoin(process(lhs), process(rhs), predicates, header)

      case flat.Distinct(fields, in, _) =>
        producer.planDistinct(process(in), fields)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case op@flat.Expand(source, rel, direction, target, sourceOp, targetOp, header, relHeader) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val externalGraph = sourceOp.sourceGraph match {
          case e: LogicalExternalGraph => e
          case _ => throw IllegalArgumentException("a LogicalExternalGraph", sourceOp.sourceGraph)
        }

        val startFrom = producer.planStartFromUnit(externalGraph)
        val second = producer.planRelationshipScan(startFrom, op.sourceGraph, rel, relHeader)

        direction match {
          case Directed =>
            producer.planExpandSource(first, second, third, source, rel, target, header)
          case Undirected =>
            val outgoing = producer.planExpandSource(first, second, third, source, rel, target, header)
            val incoming = producer.planExpandSource(third, second, first, target, rel, source, header, removeSelfRelationships = true)
            producer.planUnion(outgoing, incoming)
        }

      case op@flat.ExpandInto(source, rel, target, direction, sourceOp, header, relHeader) =>
        val in = process(sourceOp)
        val relationships = producer.planRelationshipScan(in, op.sourceGraph, rel, relHeader)

        direction match {
          case Directed =>
            producer.planExpandInto(in, relationships, source, rel, target, header)
          case Undirected =>
            val outgoing = producer.planExpandInto(in, relationships, source, rel, target, header)
            val incoming = producer.planExpandInto(in, relationships, target, rel, source, header)
            producer.planUnion(outgoing, incoming)
        }

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        producer.planInitVarExpand(process(in), source, edgeList, endNode, header)

      case flat.BoundedVarExpand(
      rel, edgeList, target, direction,
      lower, upper,
      sourceOp, relOp, targetOp,
      header, isExpandInto) =>
        val first = process(sourceOp)
        val second = process(relOp)
        val third = process(targetOp)

        producer.planBoundedVarExpand(
          first,
          second,
          third,
          rel,
          edgeList,
          target,
          sourceOp.endNode,
          lower,
          upper,
          direction,
          header,
          isExpandInto)

      case flat.Optional(lhs, rhs, header) =>
        producer.planOptional(process(lhs), process(rhs), header)

      case flat.ExistsSubQuery(predicateField, lhs, rhs, header) =>
        producer.planExistsSubQuery(process(lhs), process(rhs), predicateField, header)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header) =>
        producer.planOrderBy(process(in), sortItems, header)

      case flat.Skip(expr, in, header) =>
        producer.planSkip(process(in), expr, header)

      case flat.Limit(expr, in, header) =>
        producer.planLimit(process(in), expr, header)

      case x =>
        throw NotImplementedException(s"Physical planning of operator $x")
    }
  }

  private def relTypes(r: Var): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}
