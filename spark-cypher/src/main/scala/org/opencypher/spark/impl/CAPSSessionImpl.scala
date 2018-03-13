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
package org.opencypher.spark.impl

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.opencypher.okapi.api.graph._
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.api.value._
import org.opencypher.okapi.impl.io.SessionPropertyGraphDataSource
import org.opencypher.okapi.impl.util.Measurement.time
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.ir.api.{IRCatalogGraph, IRField}
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintFlatPlan, PrintPhysicalPlan, PrintQueryExecutionStages}
import org.opencypher.okapi.relational.impl.flat.{FlatPlanner, FlatPlannerContext}
import org.opencypher.okapi.relational.impl.physical.PhysicalPlanner
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.physical._

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

  def catalog(qualifiedGraphName: QualifiedGraphName): PropertyGraphDataSource = {
    dataSourceMapping(qualifiedGraphName.namespace)
  }

  override def cypher(query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult =
    cypherOnGraph(CAPSGraph.empty, query, parameters, drivingTable)

  override def cypherOnGraph(graph: PropertyGraph, query: String, queryParameters: CypherMap, maybeDrivingTable: Option[CypherRecords]): CypherResult = {
    val ambientGraphNew = mountAmbientGraph(graph)

    val drivingTable = maybeDrivingTable.getOrElse(CAPSRecords.unit())
    val inputFields = drivingTable.asCaps.header.internalHeader.fields

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query, inputFields)(CypherParser.defaultContext))

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = queryParameters ++ extractedParameters

    logStageProgress("IR translation ...", newLine = false)
    val irBuilderContext = IRBuilderContext.initial(query, allParameters, semState, ambientGraphNew, dataSource, inputFields)
    val ir = time("IR translation")(IRBuilder(stmt)(irBuilderContext))
    logStageProgress("Done!")

    logStageProgress("Logical planning ...", newLine = false)
    val logicalPlannerContext = LogicalPlannerContext(graph.schema, inputFields, catalog)
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
      println(optimizedLogicalPlan.pretty())
    }

    plan(drivingTable, allParameters, optimizedLogicalPlan)
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

  override def filter(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val filter = producer.planFilter(expr, scan)
    plan(in, queryParameters, filter).getRecords
  }

  override def select(
    graph: PropertyGraph,
    in: CypherRecords,
    fields: IndexedSeq[Var],
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val select = producer.planSelect(fields, scan)
    plan(in, queryParameters, select).getRecords
  }

  override def project(
    graph: PropertyGraph,
    in: CypherRecords,
    expr: Expr,
    queryParameters: CypherMap): CAPSRecords = {
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val project = producer.projectExpr(expr, scan)
    plan(in, queryParameters, project).getRecords
  }

  override def alias(
    graph: PropertyGraph,
    in: CypherRecords,
    alias: (Expr, Var),
    queryParameters: CypherMap): CAPSRecords = {
    val (expr, v) = alias
    val scan = planStart(graph, in.asCaps.header.internalHeader.fields)
    val select = producer.projectField(IRField(v.name)(v.cypherType), expr, scan)
    plan(in, queryParameters, select).getRecords
  }

  private def plan(
    records: CypherRecords,
    parameters: CypherMap,
    logicalPlan: LogicalOperator): CAPSResult = {
    logStageProgress("Flat planning ... ", newLine = false)
    val flatPlannerContext = FlatPlannerContext(parameters)
    val flatPlan = time("Flat planning")(flatPlanner(logicalPlan)(flatPlannerContext))
    logStageProgress("Done!")
    if (PrintFlatPlan.isSet) {
      println("Flat plan:")
      println(flatPlan)
    }

    logStageProgress("Physical planning ... ", newLine = false)
    val physicalPlannerContext = CAPSPhysicalPlannerContext.from(this.graph, records.asCaps, parameters)(self)
    val physicalPlan = time("Physical planning")(physicalPlanner(flatPlan)(physicalPlannerContext))
    logStageProgress("Done!")
    if (PrintPhysicalPlan.isSet) {
      println("Physical plan:")
      println(physicalPlan.pretty())
    }


    logStageProgress("Physical optimization ... ", newLine = false)
    val optimizedPhysicalPlan = time("Physical optimization")(physicalOptimizer(physicalPlan)(PhysicalOptimizerContext()))
    logStageProgress("Done!")
    if (PrintPhysicalPlan.isSet) {
      println("Optimized physical plan:")
      println(optimizedPhysicalPlan.pretty())
    }

    CAPSResultBuilder.from(logicalPlan, flatPlan, optimizedPhysicalPlan)(
      CAPSRuntimeContext(physicalPlannerContext.parameters, graphAt, collection.mutable.Map.empty))
  }

  private def mountAmbientGraph(ambient: PropertyGraph): IRCatalogGraph = {
    val graphName = GraphName(UUID.randomUUID().toString)
    val qualifiedGraphName = store(graphName, ambient)
    IRCatalogGraph(qualifiedGraphName, ambient.schema)
  }

  private def planStart(graph: PropertyGraph, fields: Set[Var]): LogicalOperator = {
    val ambientGraph = mountAmbientGraph(graph)

    producer.planStart(LogicalCatalogGraph(ambientGraph.qualifiedName, graph.schema), fields)
  }

  override def toString: String = s"${this.getClass.getSimpleName}"
}
