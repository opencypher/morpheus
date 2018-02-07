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
package org.opencypher.caps.impl.physical

import org.opencypher.caps.api.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.caps.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.caps.api.physical.{PhysicalOperator, PhysicalOperatorProducer, PhysicalPlannerContext, RuntimeContext}
import org.opencypher.caps.api.types.CTRelationship
import org.opencypher.caps.impl.flat
import org.opencypher.caps.impl.flat.FlatOperator
import org.opencypher.caps.impl.record.CypherRecords
import org.opencypher.caps.ir.api.block.SortItem
import org.opencypher.caps.ir.api.expr.{Expr, TrueLit, Var}
import org.opencypher.caps.ir.api.util.DirectCompilationStage
import org.opencypher.caps.logical.impl._

class PhysicalPlanner[P <: PhysicalOperator[R, G, C], R <: CypherRecords, G <: PropertyGraph, C <: RuntimeContext[R, G] ](producer: PhysicalOperatorProducer[P, R, G, C])

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