package org.opencypher.spark.impl.physical

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{ConstantRef, GlobalsRegistry}
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.flat.FlatOperator
import org.opencypher.spark.impl.flat
import org.opencypher.spark.impl.DirectCompilationStage
import org.opencypher.spark.impl.logical.DefaultGraphSource

case class PhysicalPlannerContext(defaultGraph: SparkCypherGraph, globals: GlobalsRegistry, constants: Map[ConstantRef, CypherValue], session: SparkSession) {
}

case class InternalResult(records: SparkCypherRecords, graphs: Map[String, SparkCypherGraph]) {

  def mapRecords(f: SparkCypherRecords => SparkCypherRecords): InternalResult =
    copy(records = f(records))
}

class PhysicalPlanner
  extends DirectCompilationStage[FlatOperator, SparkCypherResult, PhysicalPlannerContext] {


  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): SparkCypherResult = {

    val internal = inner(flatPlan)

    new SparkCypherResult {
      override def records: SparkCypherRecords = internal.records

      override def graph: SparkCypherGraph = context.defaultGraph

      override def result(name: String): Option[SparkCypherResult] = internal.graphs.get(name).map(g => new SparkCypherResult {
        override def records: SparkCypherRecords = ???

        override def graph: SparkCypherGraph = g

        override def result(name: String): Option[SparkCypherResult] = None
      })
    }
  }


  def inner(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext): InternalResult = {

    val producer = new RecordsProducer(RuntimeContext(context.constants, context.globals))

    import producer._

    // visit(node, context)
    //   context <- plan(children)
    //   context <- ...
    //   fuse(contexts)
    //   build plan for node
    //  return plan and context


    def innerPlan(flatPlan: FlatOperator): InternalResult =
      flatPlan match {
        case flat.Select(fields, in, header) =>
          innerPlan(in).select(fields, header)

        case flat.LoadGraph(outGraph, source) =>
          val default = source match {
            case DefaultGraphSource => source
            case _ => throw new NotImplementedError("")
          }

          InternalResult(unitTable(context.session), Map(outGraph.name -> context.defaultGraph))

        case op@flat.NodeScan(v, labels, in, header) =>
          // TODO: Recursively plan input tree

          innerPlan(in).nodeScan(op.inGraph, v, labels, header)

        case flat.Alias(expr, alias, in, header) =>
          innerPlan(in).alias(expr, alias, header)

        case flat.Filter(expr, in, header) => expr match {
          // TODO: Is it justified to treat labels separately?
          case TrueLit() => innerPlan(in)
          case e => innerPlan(in).filter(e, header)
        }

        // MATCH (a)-[r]->(b) => MATCH (a), (b), (a)-[r]->(b)
        case op@flat.ExpandSource(source, rel, types, target, in, header) =>
          val lhs = innerPlan(in)
          // TODO: where is the node label info? We could plan a filter here
          val g: SparkCypherGraph = lhs.graphs(op.inGraph.name)
          val nodeRhs = g.nodes(target)
          val relationships: SparkCypherRecords = g.relationships(rel)
          val relRhs = InternalResult(relationships, lhs.graphs).typeFilter(rel, types.relTypes, header)

//          val rhs = relRhs.joinTarget(nodeRhs).on(rel)(target)
//          val expanded = lhs.expandSource(rhs).on(source)(rel)
//
//          expanded
          ???
        case x =>
          throw new NotImplementedError(s"Can't plan operator $x yet")
      }

    innerPlan(flatPlan)
  }

  private def unitTable(session: SparkSession): SparkCypherRecords = new SparkCypherRecords {
    override def data: DataFrame = session.createDataFrame(Seq())
    override def header: RecordHeader = RecordHeader.empty
  }
}
