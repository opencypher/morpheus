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
package org.opencypher.caps.impl.spark

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.configuration.CoraConfiguration.{PrintPhysicalPlan, PrintQueryExecutionStages}
import org.opencypher.caps.api.graph.{CypherResult, PropertyGraph}
import org.opencypher.caps.api.io._
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.api.value._
import org.opencypher.caps.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.caps.impl.io.SessionPropertyGraphDataSource
import org.opencypher.caps.impl.physical.PhysicalPlanner
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.impl.spark.physical._
import org.opencypher.caps.ir.api.expr.{Expr, Var}
import org.opencypher.caps.ir.api.{IRExternalGraph, IRField}
import org.opencypher.caps.ir.impl.parse.CypherParser
import org.opencypher.caps.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.caps.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.caps.logical.impl._

sealed class CAPSSessionImpl(val sparkSession: SparkSession, val sessionNamespace: Namespace)
  extends CAPSSession
    with Serializable
    with CAPSSessionOps {

  self =>

  private implicit def capsSession = this
  private val producer = new LogicalOperatorProducer
  private val logicalPlanner = new LogicalPlanner(producer)
  private val logicalOptimizer = LogicalOptimizer
  private val flatPlanner = new FlatPlanner()
  private val physicalPlanner = new PhysicalPlanner(new CAPSPhysicalOperatorProducer()(self))
  private val physicalOptimizer = new PhysicalOptimizer()
  private val parser = CypherParser

  override def cypher(query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult =
    cypherOnGraph(CAPSGraph.empty, query, parameters, drivingTable)

  override def cypherOnGraph(graph: PropertyGraph, query: String, queryParameters: CypherMap, maybeDrivingTable: Option[CypherRecords]): CypherResult = {
    val ambientGraphNew = mountAmbientGraph(graph)

    val (stmt, extractedLiterals, semState) = parser.process(query)(CypherParser.defaultContext)

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = queryParameters ++ extractedParameters

    logStageProgress("IR ...", newLine = false)
    val ir = IRBuilder(stmt)(IRBuilderContext.initial(query, allParameters, semState, ambientGraphNew, dataSource))
    logStageProgress("Done!")

    logStageProgress("Logical plan ...", newLine = false)
    val logicalPlannerContext =
      LogicalPlannerContext(graph.schema, Set.empty, ir.model.graphs.mapValues(_.namespace).andThen(dataSource), ambientGraphNew)
    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    logStageProgress("Done!")

    logStageProgress("Optimizing logical plan ...", newLine = false)
    val optimizedLogicalPlan = logicalOptimizer(logicalPlan)(logicalPlannerContext)
    logStageProgress("Done!")

    if (PrintLogicalPlan.isSet) {
      println("Logical plan:")
      println(logicalPlan.pretty())
      println("Optimized logical plan:")
      println(optimizedLogicalPlan.pretty())
    }

    plan(CAPSRecords.unit(), allParameters, optimizedLogicalPlan)
  }

  override def sql(query: String): CAPSRecords =
    CAPSRecords.wrap(sparkSession.sql(query))


  /**
    * Unmounts the property graph associated with the given name from the session-local storage.
    *
    * @param graphName name of the graph within the session {{{session.graphName}}}
    */
  override def delete(graphName: GraphName): Unit = {
    val sessionDataSource = dataSourceMapping(SessionPropertyGraphDataSource.Namespace)
    sessionDataSource.graph(graphName).asCaps.unpersist()
    sessionDataSource.delete(graphName)
  }

  private def graphAt(qualifiedGraphName: QualifiedGraphName): Option[CAPSGraph] =
    Some(dataSource(qualifiedGraphName.namespace).graph(qualifiedGraphName.graphName).asCaps)

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

  private def mountAmbientGraph(ambient: PropertyGraph): IRExternalGraph = {
    val graphName = GraphName(UUID.randomUUID().toString)
    val qualifiedGraphName = store(graphName, ambient)
    IRExternalGraph(graphName.value, ambient.schema, qualifiedGraphName)
  }

  private def planStart(graph: PropertyGraph, fields: Set[Var]): LogicalOperator = {
    val ambientGraph = mountAmbientGraph(graph)

    producer.planStart(LogicalExternalGraph(ambientGraph.name, ambientGraph.qualifiedName, graph.schema), fields)
  }

  override def filter(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val filter = producer.planFilter(expr, scan)
    plan(in, queryParameters, filter).records
  }

  override def select(
    graph: PropertyGraph,
    in: CypherRecords,
    fields: IndexedSeq[Var],
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val select = producer.planSelect(fields, Set.empty, scan)
    plan(in, queryParameters, select).records
  }

  override def project(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val project = producer.projectExpr(expr, scan)
    plan(in, queryParameters, project).records
  }

  override def alias(
    graph: PropertyGraph,
    in: CypherRecords,
    alias: (Expr, Var),
    queryParameters: CypherMap): CAPSRecords = {
    val (expr, v) = alias
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val select = producer.projectField(IRField(v.name)(v.cypherType), expr, scan)
    plan(in, queryParameters, select).records
  }

  private def plan(
    records: CypherRecords,
    parameters: CypherMap,
    logicalPlan: LogicalOperator): CAPSResult = {
    logStageProgress("Flat plan ... ", newLine = false)
    val flatPlan = flatPlanner(logicalPlan)(FlatPlannerContext(parameters))
    logStageProgress("Done!")

    logStageProgress("Physical plan ... ", newLine = false)
    val physicalPlannerContext = CAPSPhysicalPlannerContext.from(this.graph, records.asCaps, parameters)(self)
    val physicalPlan = physicalPlanner(flatPlan)(physicalPlannerContext)
    logStageProgress("Done!")

    if (PrintPhysicalPlan.isSet) {
      println("Physical plan:")
      println(physicalPlan.pretty())
    }

    logStageProgress("Optimizing physical plan ... ", newLine = false)
    val optimizedPhysicalPlan = physicalOptimizer(physicalPlan)(PhysicalOptimizerContext())
    logStageProgress("Done!")

    if (PrintPhysicalPlan.isSet) {
      println("Optimized physical plan:")
      println(optimizedPhysicalPlan.pretty())
    }

    CAPSResultBuilder.from(logicalPlan, flatPlan, optimizedPhysicalPlan)(
      CAPSRuntimeContext(physicalPlannerContext.parameters, graphAt, collection.mutable.Map.empty))
  }

  override def toString: String = s"${this.getClass.getSimpleName}"
}
