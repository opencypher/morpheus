/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.configuration.Configuration.PrintTimings
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.api.value._
import org.opencypher.okapi.impl.graph.QGNGenerator
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.impl.util.Measurement.printTiming
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.configuration.IrConfiguration.PrintIr
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext, QueryCatalog}
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintFlatPlan, PrintOptimizedPhysicalPlan, PrintPhysicalPlan, PrintQueryExecutionStages}
import org.opencypher.okapi.relational.impl.physical.{RelationalPlanner, RelationalOptimizer}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.physical._

sealed class CAPSSessionImpl(val sparkSession: SparkSession)
  extends CAPSSession
    with Serializable
    with CAPSSessionOps {

  self =>

  private implicit def capsSession = this

  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val relationalOptimizer = new RelationalOptimizer()
  private val parser = CypherParser

  private val maxSessionGraphId: AtomicLong = new AtomicLong(0)

  override def cypher(query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult =
    cypherOnGraph(CAPSGraph.empty, query, parameters, drivingTable)

  override def cypherOnGraph(graph: PropertyGraph, query: String, queryParameters: CypherMap, maybeDrivingTable: Option[CypherRecords]): CypherResult = {
    val ambientGraphNew = mountAmbientGraph(graph)

    val drivingTable = maybeDrivingTable.getOrElse(CAPSRecords.unit())
    val inputFields = drivingTable.asCaps.header.vars

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query, inputFields)(CypherParser.defaultContext))

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = queryParameters ++ extractedParameters

    logStageProgress("IR translation ...", newLine = false)

    val irBuilderContext = IRBuilderContext.initial(query, allParameters, semState, ambientGraphNew, qgnGenerator, catalog.listSources, inputFields)
    val irOut = time("IR translation")(IRBuilder.process(stmt)(irBuilderContext))

    val ir = IRBuilder.extract(irOut)

    logStageProgress("Done!")

    if (PrintIr.isSet) {
      println("IR:")
      println(ir.pretty)
    }

    ir match {
      case cq: CypherQuery[Expr] =>
        planCypherQuery(graph, cq, allParameters, inputFields, drivingTable)

      case CreateGraphStatement(_, targetGraph, innerQueryIr) =>
        val innerResult = planCypherQuery(graph, innerQueryIr, allParameters, inputFields, drivingTable)
        val resultGraph = innerResult.getGraph

        catalog.store(targetGraph.qualifiedGraphName, resultGraph)

        CAPSResult.empty(innerResult.plans)

      case DeleteGraphStatement(_, targetGraph) =>
        catalog.delete(targetGraph.qualifiedGraphName)
        CAPSResult.empty()
    }
  }

  override def sql(query: String): CAPSRecords =
    CAPSRecords.wrap(sparkSession.sql(query))

  private def logStageProgress(s: String, newLine: Boolean = true): Unit = {
    if (PrintQueryExecutionStages.isSet) {
      if (newLine) {
        println(s)
      } else {
        val padded = s.padTo(30, " ").mkString("")
        print(padded)
      }
    }
  }

  override def filter(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.vars)
    val filter = producer.planFilter(expr, scan)
    planRelational(in, queryParameters, filter).getRecords
  }

  override def select(
    graph: PropertyGraph,
    in: CypherRecords,
    fields: List[Var],
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.vars)
    val select = producer.planSelect(fields, scan)
    planRelational(in, queryParameters, select).getRecords
  }

  override def project(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.vars)
    val project = producer.projectExpr(expr, scan)
    planRelational(in, queryParameters, project).getRecords
  }

  override def alias(
    graph: PropertyGraph,
    in: CypherRecords,
    alias: (Expr, Var),
    queryParameters: CypherMap): CAPSRecords = {
    val (expr, v) = alias
    val scan = planStart(graph, in.asCaps.header.vars)
    val select = producer.projectField(expr, IRField(v.name)(v.cypherType), scan)
    planRelational(in, queryParameters, select).getRecords
  }

  private def planCypherQuery(graph: PropertyGraph, cypherQuery: CypherQuery[Expr], allParameters: CypherMap, inputFields: Set[Var], drivingTable: CypherRecords) = {
    val LogicalPlan = planLogical(cypherQuery, graph, inputFields)
    planRelational(drivingTable, allParameters, LogicalPlan)
  }

  private def planLogical(ir: CypherQuery[Expr], graph: PropertyGraph, inputFields: Set[Var]) = {
    logStageProgress("Logical planning ...", newLine = false)
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, inputFields, catalog.listSources)
    val logicalPlan = time("Logical planning")(logicalPlanner(ir)(logicalPlannerContext))
    logStageProgress("Done!")
    if (PrintLogicalPlan.isSet) {
      println("Logical plan:")
      println(logicalPlan.pretty)
    }

    logStageProgress("Logical optimization ...", newLine = false)
    val optimizedLogicalPlan = time("Logical optimization")(logicalOptimizer(logicalPlan)(logicalPlannerContext))
    logStageProgress("Done!")
    if (PrintLogicalPlan.isSet) {
      println("Optimized logical plan:")
      optimizedLogicalPlan.show()
    }
    optimizedLogicalPlan
  }

  private def planRelational(
    records: CypherRecords,
    parameters: CypherMap,
    logicalPlan: LogicalOperator,
    queryCatalog: QueryCatalog = QueryCatalog(catalog.listSources)
  ): CAPSResult = {

    logStageProgress("Relational planning ... ", newLine = false)
    // TODO: Refactor so physical operator producer is reusable or easy to instantiate
    val physicalPlannerContext = CAPSPhysicalPlannerContext.from(queryCatalog, records.asCaps, parameters)(self)
    val graphAt = (qgn: QualifiedGraphName) => Some(catalog.graph(qgn).asCaps)
    val physicalPlanner = new RelationalPlanner(new CAPSPhysicalOperatorProducer()(CAPSRuntimeContext(physicalPlannerContext.parameters, graphAt, collection.mutable.Map.empty, collection.mutable.Map.empty)))
    val physicalPlan = time("Physical planning")(physicalPlanner(flatPlan)(physicalPlannerContext))
    logStageProgress("Done!")
    if (PrintPhysicalPlan.isSet) {
      println("Physical plan:")
      physicalPlan.show()
    }

    logStageProgress("Relational optimization ... ", newLine = false)
    val optimizedPhysicalPlan = time("Physical optimization")(relationalOptimizer(physicalPlan)(PhysicalOptimizerContext()))
    logStageProgress("Done!")
    if (PrintOptimizedPhysicalPlan.isSet) {
      println("Optimized physical plan:")
      optimizedPhysicalPlan.show()
    }

    CAPSResultBuilder.from(logicalPlan, flatPlan, optimizedPhysicalPlan)
  }

  private[opencypher] def time[T](description: String)(code: => T): T = {
    if (PrintTimings.isSet) printTiming(description)(code) else code
  }

  private[opencypher] val qgnGenerator = new QGNGenerator {
    override def generate: QualifiedGraphName = {
      QualifiedGraphName(SessionGraphDataSource.Namespace, GraphName(s"tmp#${maxSessionGraphId.incrementAndGet}"))
    }
  }

  private def mountAmbientGraph(ambient: PropertyGraph): IRCatalogGraph = {
    val qgn = qgnGenerator.generate
    catalog.store(qgn, ambient)
    IRCatalogGraph(qgn, ambient.schema)
  }

  private def planStart(graph: PropertyGraph, fields: Set[Var]): LogicalOperator = {
    val ambientGraph = mountAmbientGraph(graph)

    producer.planStart(LogicalCatalogGraph(ambientGraph.qualifiedGraphName, graph.schema), fields)
  }

  override def toString: String = s"${this.getClass.getSimpleName}"
}
