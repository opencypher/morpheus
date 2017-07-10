package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.classes.Cypher
import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.ir.global.{ConstantRef, ConstantRegistry, GlobalsRegistry, TokenRegistry}
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkCypherResult, SparkGraphSpace}
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.spark.impl.ir.global.GlobalsExtractor
import org.opencypher.spark.impl.ir.{CypherQueryBuilder, IRBuilderContext}
import org.opencypher.spark.impl.logical.{LogicalOperator, LogicalOperatorProducer, LogicalPlanner, LogicalPlannerContext}
import org.opencypher.spark.impl.parse.CypherParser
import org.opencypher.spark.impl.physical.{PhysicalPlanner, PhysicalPlannerContext}

final class SparkCypherEngine extends Cypher with Serializable {

  override type Graph = SparkCypherGraph
  override type Space = SparkGraphSpace
  override type Records = SparkCypherRecords
  override type Result = SparkCypherResult
  override type Data = DataFrame

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val flatPlanner = new FlatPlanner()
  private val physicalPlanner = new PhysicalPlanner()
  private val parser = CypherParser

  override def cypher(graph: Graph, query: String, queryParameters: Map[String, CypherValue]): Result = {
    val (stmt, extractedLiterals) = parser.process(query)(CypherParser.defaultContext)

    val globals = GlobalsExtractor(stmt, GlobalsRegistry(graph.space.tokens.registry))
    val GlobalsRegistry(tokens, constants) = globals

    val converted = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = (queryParameters ++ converted).map { case (k, v) => constants.constantRefByName(k) -> v }

    val paramsAndTypes = GlobalsExtractor.paramWithTypes(stmt)

    print("IR ... ")
    val ir = CypherQueryBuilder(stmt)(IRBuilderContext.initial(query, globals, graph.schema, paramsAndTypes))
    println("Done!")

    print("Logical plan ... ")
    val logicalPlan = logicalPlanner(ir)(LogicalPlannerContext(graph.schema, Set.empty))
    println("Done!")

    plan(graph, SparkCypherRecords.empty()(graph.space), tokens, constants, allParameters, logicalPlan)
  }

  override def filter(graph: Graph, in: Records, expr: Expr, queryParameters: Map[String, CypherValue]): Records = {
    val scan = producer.planStart(graph.schema, in.header.fields)
    val filter = producer.planFilter(expr, scan)
    plan(graph, in, queryParameters, filter).records
  }

  override def select(graph: Graph, in: Records, fields: IndexedSeq[Var], queryParameters: Map[String, CypherValue]): Records = {
    val scan = producer.planStart(graph.schema, in.header.fields)
    val select = producer.planSelect(fields, scan)
    plan(graph, in, queryParameters, select).records
  }

  private def plan(graph: SparkCypherGraph,
                   records: SparkCypherRecords,
                   queryParameters: Map[String, CypherValue],
                   logicalPlan: LogicalOperator): SparkCypherResult = {

    val globals = GlobalsRegistry(graph.space.tokens.registry)
    val allParameters = queryParameters.map { case (k, v) => globals.constants.constantRefByName(k) -> v }

    plan(graph, records, globals.tokens, globals.constants, allParameters, logicalPlan)
  }

  private def plan(graph: SparkCypherGraph,
                   records: SparkCypherRecords,
                   tokens: TokenRegistry,
                   constants: ConstantRegistry,
                   allParameters: Map[ConstantRef, CypherValue],
                   logicalPlan: LogicalOperator): SparkCypherResult = {
    // TODO: Remove dependency on globals (?) Only needed to enforce everything is known, that could be done
    //       differently
    print("Flat plan ... ")
    val flatPlan = flatPlanner(logicalPlan)(FlatPlannerContext(graph.schema, tokens, constants))
    println("Done!")

    // TODO: It may be better to pass tokens around in the physical planner explicitly (via the records)
    //       instead of just using a single global tokens instance derived from the graph space
    //
    print("Physical plan ... ")
    val physicalPlan = physicalPlanner(flatPlan)(PhysicalPlannerContext(graph, records, tokens, constants, allParameters))
    println("Done!")

    physicalPlan
  }
}
