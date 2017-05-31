package org.opencypher.spark.impl.physical

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{ConstantRef, GlobalsRegistry}
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.{DirectCompilationStage, flat}
import org.opencypher.spark.impl.flat.FlatOperator
import org.opencypher.spark.impl.logical.DefaultGraphSource

case class PhysicalPlannerContext(
  defaultGraph: SparkCypherGraph,
  globals: GlobalsRegistry,
  constants: Map[ConstantRef, CypherValue]) {

  val session = defaultGraph.space.session
}

class PhysicalPlanner extends DirectCompilationStage[FlatOperator, SparkCypherResult, PhysicalPlannerContext] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): SparkCypherResult = {

    val internal = inner(flatPlan)

    ResultBuilder.from(internal)
  }

  def inner(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): InternalResult = {

    val producer = new PhysicalProducer(RuntimeContext(context.constants, context.globals))
    import producer._

    flatPlan match {
      case flat.Select(fields, in, header) =>
        inner(in).select(fields, header)

      case flat.LoadGraph(outGraph, source) => source match {
        case DefaultGraphSource =>
          InternalResult(unitTable(context.session), Map(outGraph.name -> context.defaultGraph))
        case _ =>
          throw new NotImplementedError(s"Unable to load graph source other than default, got $source")
      }

      case op@flat.NodeScan(v, labels, in, header) =>
        inner(in).nodeScan(op.inGraph, v, labels, header)

      case flat.Alias(expr, alias, in, header) =>
        inner(in).alias(expr, alias, header)

      case flat.Filter(expr, in, header) => expr match {
        case TrueLit() => inner(in) // optimise away filter
        case e => inner(in).filter(e, header)
      }

      // MATCH (a)-[r]->(b) => MATCH (a), (b), (a)-[r]->(b)
      case op@flat.ExpandSource(source, rel, types, target, in, header) =>
        val lhs = inner(in)
        val g: SparkCypherGraph = lhs.graphs(op.inGraph.name)

        // TODO: Plan this in nested plan -- make Expand a binary operator
        val nodeRhs = g.nodes(target)
        val resultFromRhs = InternalResult(nodeRhs, lhs.graphs)

        val relationships: SparkCypherRecords = g.relationships(rel)
        val relRhs = InternalResult(relationships, lhs.graphs).typeFilter(rel, types.relTypes, header)

        val rhs = relRhs.joinTarget(resultFromRhs).on(rel)(target)
        val expanded = lhs.expandSource(rhs).on(source)(rel)

        expanded
      case x =>
        throw new NotImplementedError(s"Can't plan operator $x yet")
    }

  }

  private def unitTable(session: SparkSession): SparkCypherRecords = new SparkCypherRecords {
    override def data: DataFrame = session.createDataFrame(Seq())

    override def header: RecordHeader = RecordHeader.empty
  }
}
