package org.opencypher.spark.impl.physical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{ConstantRef, ConstantRegistry, TokenRegistry}
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult, SparkGraphSpace}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.flat.FlatOperator
import org.opencypher.spark.impl.logical.DefaultGraphSource
import org.opencypher.spark.impl.{DirectCompilationStage, flat}

case class PhysicalPlannerContext(
  defaultGraph: SparkCypherGraph,
  defaultRecords: SparkCypherRecords,
  tokens: TokenRegistry,
  constants: ConstantRegistry,
  parameters: Map[ConstantRef, CypherValue]) {

  val space = defaultGraph.space
  val session = space.session
}

class PhysicalPlanner extends DirectCompilationStage[FlatOperator, SparkCypherResult, PhysicalPlannerContext] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): SparkCypherResult = {

    val internal = inner(flatPlan)

    ResultBuilder.from(internal)
  }

  def inner(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): InternalResult = {

    import context.tokens

    val producer = new PhysicalProducer(RuntimeContext(context.parameters, context.tokens, context.constants))
    import producer._

    flatPlan match {
      case flat.Select(fields, in, header) =>
        inner(in).select(fields, header)

      case flat.Start(outGraph, source, _) => source match {
        case DefaultGraphSource =>
          InternalResult(context.defaultRecords, Map(outGraph.name -> context.defaultGraph))
        case _ =>
          Raise.notYetImplemented(s"Loading a graph source other than default, tried $source")
      }

      case op@flat.NodeScan(v, labels, in, header) =>
        inner(in).nodeScan(op.inGraph, v, labels, header)

      case flat.Alias(expr, alias, in, header) =>
        inner(in).alias(expr, alias, header)

      case flat.Project(expr, in, header) =>
        inner(in).project(expr, header)

      case flat.Filter(expr, in, header) => expr match {
        case TrueLit() => inner(in) // optimise away filter
        case e => inner(in).filter(e, header)
      }

      // MATCH (a)-[r]->(b) => MATCH (a), (b), (a)-[r]->(b)
      case op@flat.ExpandSource(source, rel, types, target, sourceOp, targetOp, header, relHeader) =>
        val lhs = inner(sourceOp)
        val rhs = inner(targetOp)

        val g = lhs.graphs(op.inGraph.name)
        val relationships = g.relationships(rel.name)
        val relRhs = InternalResult(relationships, lhs.graphs).typeFilter(rel, types.relTypes.map(tokens.relTypeRef), relHeader)

        val relAndTarget = relRhs.joinTarget(rhs).on(rel)(target)
        val expanded = lhs.expandSource(relAndTarget, header).on(source)(rel)

        expanded
      case x =>
        Raise.notYetImplemented(s"operator $x")
    }
  }
}
