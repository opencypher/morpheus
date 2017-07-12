package org.opencypher.spark.impl.physical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.api.ir.pattern.AnyGiven
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult, SparkGraphSpace}
import org.opencypher.spark.api.types.CTRelationship
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.flat.FlatOperator
import org.opencypher.spark.impl.logical.DefaultGraphSource
import org.opencypher.spark.impl.{DirectCompilationStage, flat}

case class PhysicalPlannerContext(
  defaultGraph: SparkCypherGraph,
  inputRecords: SparkCypherRecords,
  tokens: TokenRegistry,
  constants: ConstantRegistry,
  parameters: Map[ConstantRef, CypherValue]) {

  val space = defaultGraph.space
  val session = space.session
}

class PhysicalPlanner extends DirectCompilationStage[FlatOperator, PhysicalResult, PhysicalPlannerContext] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): PhysicalResult = {
    inner(flatPlan)
  }

  def inner(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): PhysicalResult = {

    import context.tokens

    val producer = new PhysicalResultProducer(RuntimeContext(context.parameters, context.tokens, context.constants))
    import producer._

    flatPlan match {
      case flat.Select(fields, in, header) =>
        inner(in).select(fields, header)

      case flat.Start(outGraph, source, _) => source match {
        case DefaultGraphSource =>
          PhysicalResult(context.inputRecords, Map(outGraph.name -> context.defaultGraph))
        case _ =>
          Raise.notYetImplemented(s"Loading a graph source other than default, tried $source")
      }

      case op@flat.NodeScan(v, labels, in, header) =>
        inner(in).nodeScan(op.inGraph, v, labels, header)

      case op@flat.EdgeScan(e, edgeDef, in, header) =>
        inner(in).relationshipScan(op.inGraph, e, header).typeFilter(e, edgeDef.relTypes.map(context.tokens.relTypeRef), header)

      case flat.Alias(expr, alias, in, header) =>
        inner(in).alias(expr, alias, header)

      case flat.Project(expr, in, header) =>
        inner(in).project(expr, header)

      case flat.Filter(expr, in, header) => expr match {
        case TrueLit() => inner(in) // optimise away filter
        case _ => inner(in).filter(expr, header)
      }

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of using the graph
      // MATCH (a)-[r]->(b) => MATCH (a), (b), (a)-[r]->(b)
      case op@flat.ExpandSource(source, rel, types, target, sourceOp, targetOp, header, relHeader) =>
        val lhs = inner(sourceOp)
        val rhs = inner(targetOp)

        val g = lhs.graphs(op.inGraph.name)
        val relationships = g.relationships(rel.name)
        val relRhs = PhysicalResult(relationships, lhs.graphs).typeFilter(rel, types.relTypes.map(tokens.relTypeRef), relHeader)

        val relAndTargetHeader = relRhs.records.details.header ++ rhs.records.details.header
        val relAndTarget = relRhs.joinTarget(rhs, relAndTargetHeader).on(rel)(target)
        val expanded = lhs.joinSource(relAndTarget, header).on(source)(rel)

        expanded

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        val prev = inner(in)
        prev.initVarExpand(source, edgeList, endNode, header)


      case flat.BoundedVarExpand(rel, edgeList, target, lower, upper, sourceOp, relOp, targetOp, header) =>
        val first  = inner(sourceOp)
        val second = inner(relOp)
        val third  = inner(targetOp)

        val expanded = first.varExpand(second, edgeList, sourceOp.endNode, rel, lower, upper, header)

        val joinHeader = first.records.details.header ++ third.records.details.header
        expanded.joinNode(third, joinHeader).on(sourceOp.endNode)(target)

      case x =>
        Raise.notYetImplemented(s"operator $x")
    }
  }

  private def relTypes(r: Var, tokens: TokenRegistry): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}
