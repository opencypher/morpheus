package org.opencypher.spark.impl.flat

import org.opencypher.spark.api.ir.global.{ConstantRegistry, RelType, TokenRegistry}
import org.opencypher.spark.api.ir.pattern.{AnyGiven, AnyOf, EveryRelationship}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTList, CTRelationship, CypherType}
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.logical.{DefaultGraphSource, LogicalOperator}
import org.opencypher.spark.impl.{DirectCompilationStage, logical}

import scala.annotation.tailrec

final case class FlatPlannerContext(schema: Schema, tokens: TokenRegistry, constants: ConstantRegistry)

class FlatPlanner extends DirectCompilationStage[LogicalOperator, FlatOperator, FlatPlannerContext] {

  override def process(input: LogicalOperator)(implicit context: FlatPlannerContext): FlatOperator = {
    val producer = new FlatOperatorProducer()

    input match {

      case logical.Sanitize(in) =>
        producer.sanitize(process(in))

      case logical.Select(fields, in) =>
        producer.select(fields, process(in))

      case logical.Filter(expr, in) =>
        producer.filter(expr, process(in))

      case logical.NodeScan(node, nodeDef, in) =>
        producer.nodeScan(node, nodeDef, process(in))

      case logical.Project(it, in) =>
        producer.project(it, process(in))

      case logical.ExpandSource(source, rel, types, target, sourceOp, targetOp) =>
        producer.expandSource(source, rel, types, target, process(sourceOp), process(targetOp))

      case logical.Start(outGraph, source, fields) =>
        producer.planStart(outGraph, source, fields)

      case logical.BoundedVarLengthExpand(source, edgeList, target, lower, upper, sourceOp, targetOp) =>
        val initVarExpand = producer.initVarExpand(source, edgeList, process(sourceOp))
        val types: Set[RelType] = relTypeFromList(edgeList.cypherType).map(context.tokens.relTypeByName)
        val edgeScan = producer.varLengthEdgeScan(edgeList, EveryRelationship(AnyGiven(types)), producer.planStart(initVarExpand.inGraph, DefaultGraphSource, Set.empty))
        producer.boundedVarExpand(edgeScan.edge, edgeList, target, lower, upper, initVarExpand,
          edgeScan, process(targetOp))

      case x =>
        Raise.notYetImplemented(s"Flat planning not done yet for $x")
    }
  }

  @tailrec
  private def relTypeFromList(t: CypherType): Set[String] = t match {
    case l: CTList => relTypeFromList(l.elementType)
    case r: CTRelationship => r.types
    case _ => Raise.impossible()
  }
}
