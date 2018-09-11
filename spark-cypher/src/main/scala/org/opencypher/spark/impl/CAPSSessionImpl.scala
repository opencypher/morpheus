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
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.impl.parse.CypherParser
import org.opencypher.okapi.ir.impl.{IRBuilder, IRBuilderContext}
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.{PrintOptimizedRelationalPlan, PrintQueryExecutionStages, PrintRelationalPlan}
import org.opencypher.okapi.relational.api.planning.{RelationalCypherResult, RelationalRuntimeContext}
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.planning.{RelationalOptimizer, RelationalPlanner}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable

sealed class CAPSSessionImpl(val sparkSession: SparkSession) extends CAPSSession with Serializable {

  override type Result = RelationalCypherResult[DataFrameTable]

  private val logicalPlanner = new LogicalPlanner(new LogicalOperatorProducer)
  private val logicalOptimizer = LogicalOptimizer
  private val parser = CypherParser

  private val maxSessionGraphId: AtomicLong = new AtomicLong(0)

  override def cypher(query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): Result =
    cypherOnGraph(graphs.empty, query, parameters, drivingTable)

  override def cypherOnGraph(
    graph: PropertyGraph,
    query: String,
    queryParameters: CypherMap,
    maybeDrivingTable: Option[CypherRecords]
  ): Result = {
    val ambientGraphNew = mountAmbientGraph(graph)

    val maybeCapsRecords = maybeDrivingTable.map(_.asCaps)

    val inputFields = maybeCapsRecords match {
      case Some(inputRecords) => inputRecords.header.vars
      case None => Set.empty[Var]
    }

    val (stmt, extractedLiterals, semState) = time("AST construction")(parser.process(query, inputFields)(CypherParser.defaultContext))

    val extractedParameters: CypherMap = extractedLiterals.mapValues(v => CypherValue(v))
    val allParameters = queryParameters ++ extractedParameters

    logStageProgress("IR translation ...", newLine = false)

    val irBuilderContext = IRBuilderContext.initial(query, allParameters, semState, ambientGraphNew, qgnGenerator, catalog.listSources, inputFields)
    val irOut = time("IR translation")(IRBuilder.process(stmt)(irBuilderContext))

    val ir = IRBuilder.extract(irOut)

    logStageProgress("Done!")

    ir match {
      case cq: CypherQuery =>
        if (PrintIr.isSet) {
          println("IR:")
          println(cq.pretty)
        }
        planCypherQuery(graph, cq, allParameters, inputFields, maybeCapsRecords)

      case CreateGraphStatement(_, targetGraph, innerQueryIr) =>
        val innerResult = planCypherQuery(graph, innerQueryIr, allParameters, inputFields, maybeCapsRecords)
        val resultGraph = innerResult.graph
        catalog.store(targetGraph.qualifiedGraphName, resultGraph)
        RelationalCypherResult.empty

      case CreateViewStatement(_, qgn, parameterNames, queryString) =>
        catalog.store(qgn, parameterNames, queryString)
        RelationalCypherResult.empty

      case DeleteGraphStatement(_, targetGraph) =>
        catalog.delete(targetGraph.qualifiedGraphName)
        RelationalCypherResult.empty
    }
  }

  override def sql(query: String): CAPSRecords =
    records.wrap(sparkSession.sql(query))

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

  private def planCypherQuery(
    graph: PropertyGraph,
    cypherQuery: CypherQuery,
    allParameters: CypherMap,
    inputFields: Set[Var],
    maybeDrivingTable: Option[RelationalCypherRecords[DataFrameTable]]
  ): Result = {
    val logicalPlan = planLogical(cypherQuery, graph, inputFields)
    planRelational(maybeDrivingTable, allParameters, logicalPlan)
  }

  private def planLogical(ir: CypherQuery, graph: PropertyGraph, inputFields: Set[Var]) = {
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
    maybeDrivingTable: Option[RelationalCypherRecords[DataFrameTable]],
    parameters: CypherMap,
    logicalPlan: LogicalOperator
  ): Result = {

    logStageProgress("Relational planning ... ", newLine = false)
    implicit val context: RelationalRuntimeContext[DataFrameTable] = RelationalRuntimeContext(graphAt, maybeDrivingTable, parameters)

    val relationalPlan = time("Relational planning")(RelationalPlanner.process(logicalPlan))
    logStageProgress("Done!")
    if (PrintRelationalPlan.isSet) {
      println("Relational plan:")
      relationalPlan.show()
    }

    logStageProgress("Relational optimization ... ", newLine = false)
    val optimizedRelationalPlan = time("Relational optimization")(RelationalOptimizer.process(relationalPlan))
    logStageProgress("Done!")
    if (PrintOptimizedRelationalPlan.isSet) {
      println("Optimized Relational plan:")
      optimizedRelationalPlan.show()
    }

    RelationalCypherResult(logicalPlan, optimizedRelationalPlan)
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

  override def toString: String = s"${this.getClass.getSimpleName}"

}
